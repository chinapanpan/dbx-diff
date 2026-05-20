"""
Databricks vs EMR Data Comparison Tool.

Reads table configs from a CSV, compares data between Databricks (Delta on S3)
and EMR (Spark SQL), outputs diff report as Markdown to S3.
"""

import sys
import os
import time
import datetime
import csv
import json
import threading
import tempfile
import argparse
import requests
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit


DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-51ad87e6-c26d.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
MAX_WORKERS = 15
TIMEOUT_PER_TABLE = 600  # 10 minutes max per table comparison
EMR_CATALOG = "iceberg_catalog"  # default, overridable via --emr-catalog

_write_lock = threading.Lock()


def map_to_emr_table(dbx_table_name: str, emr_catalog: str) -> str:
    """Map Databricks table name (catalog.schema.table) to EMR table name."""
    parts = dbx_table_name.split(".")
    if len(parts) == 3:
        # workspace.demo2.table -> iceberg_catalog.demo2.table
        return f"{emr_catalog}.{parts[1]}.{parts[2]}"
    return dbx_table_name


def get_spark(emr_catalog: str = "iceberg_catalog", warehouse: str = "s3://zpf-databricks-event/emr/demo2") -> SparkSession:
    return SparkSession.builder \
        .appName("DbxDiff") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config(f"spark.sql.catalog.{emr_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{emr_catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{emr_catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config(f"spark.sql.catalog.{emr_catalog}.warehouse", warehouse) \
        .getOrCreate()


def get_table_location(table_name: str) -> str:
    """Get S3 storage location of a Databricks Unity Catalog table via REST API."""
    url = f"{DATABRICKS_HOST}/api/2.1/unity-catalog/tables/{table_name}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"Failed to get table info for {table_name}: {resp.status_code} {resp.text}")
    info = resp.json()
    location = info.get("storage_location")
    if not location:
        raise Exception(f"No storage_location found for table {table_name}")
    return location


def get_table_columns_dbx(table_name: str) -> Tuple[List[str], List[str]]:
    """Get column names and partition columns from Databricks Unity Catalog API."""
    url = f"{DATABRICKS_HOST}/api/2.1/unity-catalog/tables/{table_name}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"Failed to get table info for {table_name}: {resp.status_code} {resp.text}")
    info = resp.json()
    columns = info.get("columns", [])
    data_cols = []
    part_cols = []
    for c in columns:
        name = c.get("name", "")
        if c.get("partition_index") is not None:
            part_cols.append(name)
        else:
            data_cols.append(name)
    return data_cols, part_cols


def read_delta_table(spark: SparkSession, s3_location: str) -> DataFrame:
    """Read a Delta table from S3 location."""
    return spark.read.format("delta").load(s3_location)


def read_emr_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a table from EMR Spark catalog (Iceberg/Hive via Glue)."""
    return spark.table(table_name)


def parse_csv(csv_path: str) -> List[Dict[str, str]]:
    """Parse input CSV file (comma-delimited)."""
    rows = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                'table_name': row['table_name'].strip(),
                'primary_keys': row.get('primary_keys', '').strip(),
                'pt_start': row.get('pt_start', '').strip(),
                'pt_end': row.get('pt_end', '').strip(),
                'pt_keys': row.get('pt_keys', '').strip(),
            })
    return rows


def append_md(report_path: str, content: str):
    """Thread-safe append to markdown report file."""
    with _write_lock:
        with open(report_path, 'a') as f:
            f.write(content)
            f.flush()


def upload_to_s3(local_path: str, s3_path: str):
    """Upload local file to S3 using boto3."""
    import boto3
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    s3.upload_file(local_path, bucket, key)
    print(f"Uploaded report to {s3_path}")


def compare_counts(df_dbx: DataFrame, df_emr: DataFrame, table_name: str,
                   pt_val: Optional[str] = None) -> Dict:
    """Compare row counts between two DataFrames."""
    dbx_count = df_dbx.count()
    emr_count = df_emr.count()
    result = {
        'table': table_name,
        'pt': pt_val,
        'dbx_count': dbx_count,
        'emr_count': emr_count,
        'match': dbx_count == emr_count,
    }
    return result


def compare_rows_by_keys(df_dbx: DataFrame, df_emr: DataFrame,
                         primary_keys: List[str], data_cols: List[str],
                         table_name: str, pt_val: Optional[str] = None,
                         sample_limit: int = 10) -> Dict:
    """Compare rows by primary keys, returning mismatched samples."""
    from functools import reduce

    key_cols = primary_keys
    non_key_cols = [c for c in data_cols if c not in key_cols]

    # Rename columns with prefix to avoid ambiguity after join
    df_dbx_prefixed = df_dbx.select(
        *[col(k).alias(k) for k in key_cols],
        *[col(c).alias(f"dbx_{c}") for c in non_key_cols]
    )
    df_emr_prefixed = df_emr.select(
        *[col(k).alias(k) for k in key_cols],
        *[col(c).alias(f"emr_{c}") for c in non_key_cols]
    )

    # Find rows only in Databricks
    only_dbx = df_dbx.join(df_emr, key_cols, "left_anti")
    only_dbx_count = only_dbx.count()
    only_dbx_samples = only_dbx.limit(sample_limit).collect() if only_dbx_count > 0 else []

    # Find rows only in EMR
    only_emr = df_emr.join(df_dbx, key_cols, "left_anti")
    only_emr_count = only_emr.count()
    only_emr_samples = only_emr.limit(sample_limit).collect() if only_emr_count > 0 else []

    # Find rows in both but with different values
    joined = df_dbx_prefixed.join(df_emr_prefixed, key_cols, "inner")
    mismatched_conditions = []
    for c in non_key_cols:
        mismatched_conditions.append(
            (col(f"dbx_{c}").isNull() & col(f"emr_{c}").isNotNull()) |
            (col(f"dbx_{c}").isNotNull() & col(f"emr_{c}").isNull()) |
            (col(f"dbx_{c}") != col(f"emr_{c}"))
        )

    if mismatched_conditions:
        filter_expr = reduce(lambda a, b: a | b, mismatched_conditions)
        mismatched = joined.filter(filter_expr)
        mismatched_count = mismatched.count()
        mismatched_samples = []
        if mismatched_count > 0:
            sample_rows = mismatched.limit(sample_limit).collect()
            for row in sample_rows:
                row_dict = row.asDict()
                diffs = {}
                for c in non_key_cols:
                    dbx_val = row_dict.get(f"dbx_{c}")
                    emr_val = row_dict.get(f"emr_{c}")
                    if dbx_val != emr_val:
                        diffs[c] = {'dbx': str(dbx_val), 'emr': str(emr_val)}
                if diffs:
                    keys_info = {k: str(row_dict.get(k, '')) for k in key_cols}
                    mismatched_samples.append({'keys': keys_info, 'diffs': diffs})
    else:
        mismatched_count = 0
        mismatched_samples = []

    return {
        'table': table_name,
        'pt': pt_val,
        'only_dbx_count': only_dbx_count,
        'only_emr_count': only_emr_count,
        'mismatched_count': mismatched_count,
        'only_dbx_samples': [r.asDict() for r in only_dbx_samples],
        'only_emr_samples': [r.asDict() for r in only_emr_samples],
        'mismatched_samples': mismatched_samples,
        'match': only_dbx_count == 0 and only_emr_count == 0 and mismatched_count == 0,
    }


def format_count_result_md(result: Dict) -> str:
    """Format count comparison result as Markdown."""
    status = "PASS" if result['match'] else "FAIL"
    pt_info = f" (pt={result['pt']})" if result.get('pt') else ""
    md = f"\n### {result['table']}{pt_info} — Count Check: **{status}**\n\n"
    md += f"| Side | Count |\n|------|-------|\n"
    md += f"| Databricks | {result['dbx_count']} |\n"
    md += f"| EMR | {result['emr_count']} |\n"
    if not result['match']:
        diff = result['dbx_count'] - result['emr_count']
        md += f"\n> Difference: {diff:+d}\n"
    return md


def format_row_result_md(result: Dict) -> str:
    """Format row-level comparison result as Markdown."""
    status = "PASS" if result['match'] else "FAIL"
    pt_info = f" (pt={result['pt']})" if result.get('pt') else ""
    md = f"\n### {result['table']}{pt_info} — Row Check: **{status}**\n\n"

    if result['match']:
        md += "> All rows match.\n"
        return md

    md += f"| Metric | Count |\n|--------|-------|\n"
    md += f"| Only in Databricks | {result['only_dbx_count']} |\n"
    md += f"| Only in EMR | {result['only_emr_count']} |\n"
    md += f"| Value Mismatched | {result['mismatched_count']} |\n"

    if result.get('only_dbx_samples'):
        md += f"\n**Samples only in Databricks** (up to 10):\n\n"
        md += "```\n"
        for s in result['only_dbx_samples'][:5]:
            md += f"  {s}\n"
        md += "```\n"

    if result.get('only_emr_samples'):
        md += f"\n**Samples only in EMR** (up to 10):\n\n"
        md += "```\n"
        for s in result['only_emr_samples'][:5]:
            md += f"  {s}\n"
        md += "```\n"

    if result.get('mismatched_samples'):
        md += f"\n**Mismatched value samples** (up to 10):\n\n"
        md += "| Keys | Column | Databricks | EMR |\n|------|--------|------------|-----|\n"
        for s in result['mismatched_samples'][:10]:
            keys_str = str(s['keys'])
            for col_name, vals in s['diffs'].items():
                md += f"| {keys_str} | {col_name} | {vals['dbx']} | {vals['emr']} |\n"

    return md


def format_partition_count_md(table_name: str, dbx_partitions: List[str], emr_partitions: List[str]) -> str:
    """Format partition count comparison as Markdown."""
    dbx_set = set(dbx_partitions)
    emr_set = set(emr_partitions)
    match = dbx_set == emr_set
    status = "PASS" if match else "FAIL"

    md = f"\n### {table_name} — Partition Count Check: **{status}**\n\n"
    md += f"| Side | Partition Count |\n|------|----------------|\n"
    md += f"| Databricks | {len(dbx_set)} |\n"
    md += f"| EMR | {len(emr_set)} |\n"

    if not match:
        only_dbx = dbx_set - emr_set
        only_emr = emr_set - dbx_set
        if only_dbx:
            md += f"\n> Partitions only in Databricks: {sorted(only_dbx)[:20]}\n"
        if only_emr:
            md += f"\n> Partitions only in EMR: {sorted(only_emr)[:20]}\n"

    return md


def compare_single_table(spark: SparkSession, config: Dict, report_path: str, emr_catalog: str):
    """Compare a single table between Databricks and EMR."""
    table_name = config['table_name']
    emr_table_name = map_to_emr_table(table_name, emr_catalog)
    primary_keys = [k.strip() for k in config['primary_keys'].split(',') if k.strip()] if config['primary_keys'] else []
    pt_start = config['pt_start'] if config['pt_start'] else None
    pt_end = config['pt_end'] if config['pt_end'] else None
    pt_keys = [k.strip() for k in config['pt_keys'].split(',') if k.strip()] if config['pt_keys'] else []

    is_partitioned = bool(pt_start and pt_end)
    has_primary_keys = bool(primary_keys)

    start_time = time.time()
    md_content = f"\n---\n## Table: `{table_name}`\n"
    md_content += f"- Primary Keys: `{primary_keys if primary_keys else 'None'}`\n"
    md_content += f"- Partitioned: {'Yes' if is_partitioned else 'No'}"
    if is_partitioned:
        md_content += f" (pt_start={pt_start}, pt_end={pt_end})"
    md_content += f"\n- Started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    append_md(report_path, md_content)

    try:
        # Get Databricks table location
        dbx_location = get_table_location(table_name)

        # Read Databricks Delta table
        df_dbx_full = read_delta_table(spark, dbx_location)

        # Read EMR table
        df_emr_full = read_emr_table(spark, emr_table_name)

        # Get column info
        dbx_cols = [c for c in df_dbx_full.columns]
        emr_cols = [c for c in df_emr_full.columns]

        # Determine data columns (excluding partition column 'pt' if partitioned)
        if is_partitioned:
            data_cols = [c for c in dbx_cols if c != 'pt']
        else:
            data_cols = dbx_cols

        if has_primary_keys and not is_partitioned:
            # Case 1: Primary keys, non-partitioned — full row-level comparison
            result = compare_rows_by_keys(df_dbx_full, df_emr_full, primary_keys, data_cols, table_name)
            append_md(report_path, format_row_result_md(result))

        elif has_primary_keys and is_partitioned:
            # Case 2: Primary keys, partitioned
            # Step 1: Compare partition counts
            dbx_partitions = [r['pt'] for r in df_dbx_full.select("pt").distinct().collect()]
            emr_partitions = [r['pt'] for r in df_emr_full.select("pt").distinct().collect()]

            # Filter to pt range
            dbx_in_range = [p for p in dbx_partitions if pt_start <= str(p) <= pt_end]
            emr_in_range = [p for p in emr_partitions if pt_start <= str(p) <= pt_end]

            append_md(report_path, format_partition_count_md(table_name, dbx_in_range, emr_in_range))

            # Step 2: If pt_keys specified, compare per-partition counts then row-level
            if pt_keys:
                for pt_val in pt_keys:
                    df_dbx_pt = df_dbx_full.filter(col("pt") == pt_val)
                    df_emr_pt = df_emr_full.filter(col("pt") == pt_val)

                    count_result = compare_counts(df_dbx_pt, df_emr_pt, table_name, pt_val)
                    append_md(report_path, format_count_result_md(count_result))

                    if count_result['match']:
                        # Counts match — do row-level comparison
                        row_result = compare_rows_by_keys(
                            df_dbx_pt, df_emr_pt, primary_keys, data_cols, table_name, pt_val
                        )
                        append_md(report_path, format_row_result_md(row_result))

        elif not has_primary_keys and not is_partitioned:
            # Case 3: No primary keys, non-partitioned — count comparison only
            count_result = compare_counts(df_dbx_full, df_emr_full, table_name)
            append_md(report_path, format_count_result_md(count_result))

        elif not has_primary_keys and is_partitioned:
            # Case 4: No primary keys, partitioned
            # Step 1: Compare partition counts
            dbx_partitions = [r['pt'] for r in df_dbx_full.select("pt").distinct().collect()]
            emr_partitions = [r['pt'] for r in df_emr_full.select("pt").distinct().collect()]

            dbx_in_range = [p for p in dbx_partitions if pt_start <= str(p) <= pt_end]
            emr_in_range = [p for p in emr_partitions if pt_start <= str(p) <= pt_end]

            append_md(report_path, format_partition_count_md(table_name, dbx_in_range, emr_in_range))

            # Step 2: If pt_keys specified, compare per-partition counts
            if pt_keys:
                for pt_val in pt_keys:
                    df_dbx_pt = df_dbx_full.filter(col("pt") == pt_val)
                    df_emr_pt = df_emr_full.filter(col("pt") == pt_val)
                    count_result = compare_counts(df_dbx_pt, df_emr_pt, table_name, pt_val)
                    append_md(report_path, format_count_result_md(count_result))

        elapsed = time.time() - start_time
        append_md(report_path, f"\n> Completed in {elapsed:.1f}s\n")

    except Exception as e:
        elapsed = time.time() - start_time
        error_md = f"\n### ERROR for `{table_name}`\n\n"
        error_md += f"```\n{str(e)}\n```\n"
        error_md += f"\n> Failed after {elapsed:.1f}s\n"
        append_md(report_path, error_md)
        print(f"ERROR processing {table_name}: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Databricks vs EMR Data Diff Tool")
    parser.add_argument("--csv", required=True, help="Path to input CSV file (space-delimited)")
    parser.add_argument("--s3-output", required=True, help="S3 path for output report (e.g. s3://bucket/path/report.md)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help=f"Number of parallel workers (default: {MAX_WORKERS})")
    parser.add_argument("--timeout", type=int, default=TIMEOUT_PER_TABLE, help=f"Timeout per table in seconds (default: {TIMEOUT_PER_TABLE})")
    parser.add_argument("--emr-catalog", default=EMR_CATALOG, help=f"EMR catalog name (default: {EMR_CATALOG})")
    parser.add_argument("--emr-warehouse", default="s3://zpf-databricks-event/emr/demo2", help="EMR Iceberg warehouse location")
    args = parser.parse_args()

    spark = get_spark(emr_catalog=args.emr_catalog, warehouse=args.emr_warehouse)

    # Parse CSV
    configs = parse_csv(args.csv)
    print(f"Loaded {len(configs)} table configs from {args.csv}")

    # Create local temp report file
    report_path = tempfile.mktemp(suffix=".md")
    header = f"# Databricks vs EMR Data Diff Report\n\n"
    header += f"- Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    header += f"- Tables: {len(configs)}\n"
    header += f"- Workers: {args.workers}\n\n"
    with open(report_path, 'w') as f:
        f.write(header)

    # Run comparisons in parallel using ThreadPoolExecutor
    total_start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for config in configs:
            future = executor.submit(compare_single_table, spark, config, report_path, args.emr_catalog)
            futures[future] = config['table_name']

        for future in as_completed(futures):
            table = futures[future]
            try:
                future.result(timeout=args.timeout)
                print(f"DONE: {table}")
            except Exception as e:
                print(f"TIMEOUT/ERROR: {table}: {e}", file=sys.stderr)
                error_md = f"\n---\n## Table: `{table}` — **TIMEOUT**\n\n"
                error_md += f"```\n{str(e)}\n```\n"
                append_md(report_path, error_md)

    total_elapsed = time.time() - total_start
    summary = f"\n---\n## Summary\n\n"
    summary += f"- Total tables: {len(configs)}\n"
    summary += f"- Total time: {total_elapsed:.1f}s\n"
    append_md(report_path, summary)

    # Upload to S3
    upload_to_s3(report_path, args.s3_output)

    # Cleanup
    os.remove(report_path)
    spark.stop()
    print(f"Done. Report uploaded to {args.s3_output}")


if __name__ == "__main__":
    main()
