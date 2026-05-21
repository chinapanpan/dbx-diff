"""
Databricks vs EMR Data Comparison Tool.

Reads table configs from a CSV, compares data between Databricks (Delta on S3)
and EMR (Spark SQL), outputs diff report as Markdown to S3.
"""

import sys
import os

try:
    import certifi
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
except ImportError:
    if not os.environ.get("SSL_CERT_FILE"):
        for ca_path in [
            "/etc/pki/tls/certs/ca-bundle.crt",
            "/etc/ssl/certs/ca-certificates.crt",
        ]:
            if os.path.exists(ca_path):
                os.environ["SSL_CERT_FILE"] = ca_path
                break

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


DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
MAX_WORKERS = 15
TIMEOUT_PER_TABLE = 600  # 10 minutes max per table comparison
EMR_CATALOG = "iceberg_catalog"  # default, overridable via --emr-catalog

_write_lock = threading.Lock()
_token_lock = threading.Lock()
_oauth2_token = None
_oauth2_expiry = 0


def map_to_emr_table(dbx_table_name: str, emr_catalog: str) -> str:
    """Map Databricks table name (catalog.schema.table) to EMR table name."""
    parts = dbx_table_name.split(".")
    if len(parts) == 3:
        # workspace.demo2.table -> iceberg_catalog.demo2.table
        return f"{emr_catalog}.{parts[1]}.{parts[2]}"
    return dbx_table_name


def get_spark(emr_catalog: str = "iceberg_catalog", warehouse: str = "s3://zpf-databricks-event/emr/demo2") -> SparkSession:
    """Get or create SparkSession. Configs here are defaults; spark-submit --conf overrides them."""
    return SparkSession.builder \
        .appName("DbxDiff") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config(f"spark.sql.catalog.{emr_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{emr_catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{emr_catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config(f"spark.sql.catalog.{emr_catalog}.warehouse", warehouse) \
        .getOrCreate()


def get_secret_from_sm(secret_arn: str, region: str = "us-west-2") -> Tuple[str, str]:
    """Retrieve client_id:client_secret from AWS Secrets Manager (plaintext, colon-separated)."""
    import boto3
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_arn)
    secret_str = resp["SecretString"].strip()
    parts = secret_str.split(":", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Secret {secret_arn} must be plaintext format: client_id:client_secret")
    print(f"Retrieved OAuth2 credentials from Secrets Manager: {secret_arn}")
    return parts[0], parts[1]


def get_oauth2_token(host: str, client_id: str, client_secret: str) -> str:
    """Get OAuth2 access token from Databricks using client credentials flow."""
    global _oauth2_token, _oauth2_expiry
    with _token_lock:
        if _oauth2_token and time.time() < _oauth2_expiry - 60:
            return _oauth2_token
        url = f"{host}/oidc/v1/token"
        data = {
            "grant_type": "client_credentials",
            "scope": "all-apis",
        }
        resp = requests.post(url, data=data, auth=(client_id, client_secret), timeout=30)
        if resp.status_code != 200:
            raise Exception(f"OAuth2 token request failed: {resp.status_code} {resp.text}")
        token_data = resp.json()
        _oauth2_token = token_data["access_token"]
        _oauth2_expiry = time.time() + token_data.get("expires_in", 3600)
        print(f"Obtained OAuth2 token, expires in {token_data.get('expires_in', 3600)}s")
        return _oauth2_token


def get_dbx_auth_headers() -> Dict[str, str]:
    """Get authorization headers for Databricks API calls."""
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


def fetch_table_metadata(table_name: str) -> Dict:
    """Fetch table metadata (location + partition columns) from Databricks Unity Catalog API."""
    url = f"{DATABRICKS_HOST}/api/2.1/unity-catalog/tables/{table_name}"
    headers = get_dbx_auth_headers()
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"Failed to get table info for {table_name}: {resp.status_code} {resp.text}")
    info = resp.json()
    location = info.get("storage_location", "")
    columns = info.get("columns", [])
    part_cols = []
    for c in columns:
        if c.get("partition_index") is not None:
            part_cols.append(c.get("name", ""))
    return {"location": location, "partition_cols": part_cols}


def prefetch_all_table_metadata(configs: List[Dict]) -> Dict[str, Dict]:
    """Batch-fetch metadata for all tables upfront. Returns dict keyed by table_name."""
    metadata = {}
    print(f"Pre-fetching metadata for {len(configs)} tables from Databricks API...")
    for i, config in enumerate(configs):
        table_name = config['table_name']
        if config.get('dbx_location'):
            metadata[table_name] = {"location": config['dbx_location'], "partition_cols": []}
            continue
        try:
            meta = fetch_table_metadata(table_name)
            metadata[table_name] = meta
        except Exception as e:
            print(f"  WARNING: Failed to fetch metadata for {table_name}: {e}", file=sys.stderr)
            metadata[table_name] = {"location": "", "partition_cols": [], "error": str(e)}
        if (i + 1) % 50 == 0:
            print(f"  Fetched {i + 1}/{len(configs)} tables")
    print(f"Pre-fetch complete. {sum(1 for v in metadata.values() if v.get('location'))} tables resolved.")
    return metadata


def read_delta_table(spark: SparkSession, s3_location: str) -> DataFrame:
    """Read a Delta table from S3 location."""
    return spark.read.format("delta").load(s3_location)


def read_emr_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a table from EMR Spark catalog (Iceberg/Hive via Glue)."""
    return spark.table(table_name)


def parse_csv(csv_path: str) -> List[Dict[str, str]]:
    """Parse input CSV file (space-delimited). Supports local or S3 paths.

    CSV columns: table_name, primary_keys, pt_start, pt_end, pt_keys, dbx_location (optional)
    If dbx_location is provided, it will be used directly instead of calling Databricks API.
    """
    if csv_path.startswith("s3://"):
        import boto3
        s3 = boto3.client("s3")
        bucket, key = csv_path.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
        import io
        f = io.StringIO(content)
    else:
        f = open(csv_path, 'r')

    rows = []
    reader = csv.DictReader(f, delimiter=' ')
    for row in reader:
        rows.append({
            'table_name': (row.get('table_name') or '').strip(),
            'primary_keys': (row.get('primary_keys') or '').strip(),
            'pt_start': (row.get('pt_start') or '').strip(),
            'pt_end': (row.get('pt_end') or '').strip(),
            'pt_keys': (row.get('pt_keys') or '').strip(),
            'dbx_location': (row.get('dbx_location') or '').strip(),
        })
    if not csv_path.startswith("s3://"):
        f.close()
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


def compare_single_table(spark: SparkSession, config: Dict, report_path: str, emr_catalog: str,
                         table_metadata: Dict = None):
    """Compare a single table between Databricks and EMR.

    table_metadata: pre-fetched dict with 'location' and 'partition_cols' from Databricks API.
    The 'partition_cols' list determines whether the table is treated as partitioned
    (must contain 'pt'), regardless of what CSV says in pt_start/pt_end.
    """
    table_name = config['table_name']
    emr_table_name = map_to_emr_table(table_name, emr_catalog)
    primary_keys = [k.strip() for k in config['primary_keys'].split(',') if k.strip()] if config['primary_keys'] else []
    pt_start = config['pt_start'] if config['pt_start'] else None
    pt_end = config['pt_end'] if config['pt_end'] else None
    pt_keys = [k.strip() for k in config['pt_keys'].split(',') if k.strip()] if config['pt_keys'] else []

    # Determine if table is actually partitioned by 'pt' based on API metadata
    meta = table_metadata or {}
    part_cols = meta.get("partition_cols", [])
    has_pt_column = "pt" in part_cols
    is_partitioned = has_pt_column and bool(pt_start and pt_end)
    has_primary_keys = bool(primary_keys)

    start_time = time.time()
    md_content = f"\n---\n## Table: `{table_name}`\n"
    md_content += f"- Primary Keys: `{primary_keys if primary_keys else 'None'}`\n"
    md_content += f"- Partitioned: {'Yes' if is_partitioned else 'No'}"
    if is_partitioned:
        md_content += f" (pt_start={pt_start}, pt_end={pt_end})"
    elif pt_start and pt_end and not has_pt_column:
        md_content += f" (CSV has pt_start/pt_end but table has no 'pt' partition column — treating as non-partitioned)"
    md_content += f"\n- Started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    append_md(report_path, md_content)

    try:
        # Get Databricks table location from pre-fetched metadata
        dbx_location = meta.get("location", "")
        if not dbx_location:
            if meta.get("error"):
                raise Exception(f"Pre-fetch failed: {meta['error']}")
            raise Exception(f"No storage_location found for table {table_name}")

        # Read Databricks Delta table
        df_dbx_full = read_delta_table(spark, dbx_location)

        # Read EMR table
        df_emr_full = read_emr_table(spark, emr_table_name)

        # Get column info
        dbx_cols = [c for c in df_dbx_full.columns]

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
    parser.add_argument("--databricks-host", default=None, help="Databricks workspace URL (overrides DATABRICKS_HOST env)")
    parser.add_argument("--databricks-token", default=None, help="Databricks access token (overrides DATABRICKS_TOKEN env)")
    parser.add_argument("--databricks-secret-arn", default=None,
                        help="AWS Secrets Manager ARN containing client_id:client_secret for OAuth2 (overrides --databricks-token)")
    parser.add_argument("--region", default="us-west-2", help="AWS region for Secrets Manager (default: us-west-2)")
    args = parser.parse_args()

    global DATABRICKS_HOST, DATABRICKS_TOKEN
    if args.databricks_host:
        DATABRICKS_HOST = args.databricks_host

    # Authentication: OAuth2 via secret ARN takes precedence over token
    if args.databricks_secret_arn:
        if not DATABRICKS_HOST:
            print("ERROR: --databricks-host is required when using --databricks-secret-arn", file=sys.stderr)
            sys.exit(1)
        client_id, client_secret = get_secret_from_sm(args.databricks_secret_arn, args.region)
        DATABRICKS_TOKEN = get_oauth2_token(DATABRICKS_HOST, client_id, client_secret)
    elif args.databricks_token:
        DATABRICKS_TOKEN = args.databricks_token

    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("ERROR: Databricks host and credentials must be provided via "
              "--databricks-secret-arn or --databricks-host/--databricks-token or env vars",
              file=sys.stderr)
        sys.exit(1)

    spark = get_spark(emr_catalog=args.emr_catalog, warehouse=args.emr_warehouse)

    # Parse CSV
    configs = parse_csv(args.csv)
    print(f"Loaded {len(configs)} table configs from {args.csv}")

    # Batch pre-fetch all table metadata (location + partition columns) from Databricks API
    prefetch_start = time.time()
    all_metadata = prefetch_all_table_metadata(configs)
    prefetch_elapsed = time.time() - prefetch_start
    print(f"Metadata pre-fetch took {prefetch_elapsed:.1f}s")

    # Create local temp report file
    report_path = tempfile.mktemp(suffix=".md")
    header = f"# Databricks vs EMR Data Diff Report\n\n"
    header += f"- Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    header += f"- Tables: {len(configs)}\n"
    header += f"- Workers: {args.workers}\n"
    header += f"- Metadata pre-fetch: {prefetch_elapsed:.1f}s\n\n"
    with open(report_path, 'w') as f:
        f.write(header)

    # Run comparisons in parallel using ThreadPoolExecutor
    total_start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for config in configs:
            table_meta = all_metadata.get(config['table_name'], {})
            future = executor.submit(compare_single_table, spark, config, report_path, args.emr_catalog, table_meta)
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
    summary += f"- Metadata pre-fetch: {prefetch_elapsed:.1f}s\n"
    summary += f"- Comparison time: {total_elapsed:.1f}s\n"
    summary += f"- Total time: {prefetch_elapsed + total_elapsed:.1f}s\n"
    append_md(report_path, summary)

    # Upload to S3
    upload_to_s3(report_path, args.s3_output)

    # Cleanup
    os.remove(report_path)
    spark.stop()
    print(f"Done. Report uploaded to {args.s3_output}")


if __name__ == "__main__":
    main()
