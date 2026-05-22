"""
Databricks vs EMR Data Comparison Tool (v3 - Aggregate Based).

Compares numeric column aggregates (count, sum, max, min, avg) between
Databricks (Delta on S3) and EMR (Iceberg via Glue catalog).
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
import threading
import tempfile
import argparse
import requests
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    IntegerType, LongType, FloatType, DoubleType,
    DecimalType, ShortType, ByteType
)
from pyspark.sql import functions as F


DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
MAX_WORKERS = 15
TIMEOUT_PER_TABLE = 600

_write_lock = threading.Lock()
_token_lock = threading.Lock()
_oauth2_token = None
_oauth2_expiry = 0

NUMERIC_TYPES = (IntegerType, LongType, FloatType, DoubleType, DecimalType, ShortType, ByteType)



def get_spark() -> SparkSession:
    """Get or create SparkSession. Catalog configs are set at EMR Serverless Application level."""
    return SparkSession.builder \
        .appName("DbxDiff") \
        .getOrCreate()


def get_secret_from_sm(secret_arn: str, region: str = "us-west-2") -> Tuple[str, str]:
    """Retrieve client_id:client_secret from AWS Secrets Manager."""
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
        data = {"grant_type": "client_credentials", "scope": "all-apis"}
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
    """Batch-fetch metadata for all tables upfront."""
    metadata = {}
    print(f"Pre-fetching metadata for {len(configs)} tables from Databricks API...")
    for i, config in enumerate(configs):
        table_name = config['table_name']
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
    """Read a table from EMR Spark catalog (Iceberg via Glue)."""
    return spark.table(table_name)


def parse_csv(csv_path: str) -> List[Dict[str, str]]:
    """Parse input CSV file (space-delimited).

    CSV columns: table_name, pt_keys
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
            'pt_keys': (row.get('pt_keys') or '').strip(),
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


def identify_numeric_columns(df: DataFrame) -> List[str]:
    """Identify numeric columns from a DataFrame's schema."""
    numeric_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, NUMERIC_TYPES):
            numeric_cols.append(field.name)
    return numeric_cols


def get_column_types(df: DataFrame, numeric_cols: List[str]) -> Dict[str, str]:
    """Get data type names for numeric columns."""
    type_map = {}
    for field in df.schema.fields:
        if field.name in numeric_cols:
            type_map[field.name] = field.dataType.simpleString()
    return type_map


def build_agg_sql(table_name: str, numeric_cols: List[str], pt_keys: List[str] = None) -> str:
    """Build the SQL string for aggregate comparison (for reporting purposes)."""
    agg_parts = ["count(1) AS total_count"]
    for col_name in numeric_cols:
        agg_parts.append(f"sum({col_name}) AS {col_name}_sum")
        agg_parts.append(f"max({col_name}) AS {col_name}_max")
        agg_parts.append(f"min({col_name}) AS {col_name}_min")
        agg_parts.append(f"avg({col_name}) AS {col_name}_avg")
    select_clause = ",\n       ".join(agg_parts)

    if pt_keys:
        pt_list = ", ".join(f"'{p}'" for p in pt_keys)
        return (f"SELECT pt,\n       {select_clause}\n"
                f"FROM {table_name}\n"
                f"WHERE pt IN ({pt_list})\n"
                f"GROUP BY pt\nORDER BY pt")
    else:
        return f"SELECT {select_clause}\nFROM {table_name}"


def compute_aggregates(df: DataFrame, numeric_cols: List[str]) -> DataFrame:
    """Compute count, sum, max, min, avg for each numeric column on a DataFrame."""
    agg_exprs = [F.count(F.lit(1)).alias("total_count")]
    for col_name in numeric_cols:
        agg_exprs.append(F.sum(F.col(col_name)).alias(f"{col_name}_sum"))
        agg_exprs.append(F.max(F.col(col_name)).alias(f"{col_name}_max"))
        agg_exprs.append(F.min(F.col(col_name)).alias(f"{col_name}_min"))
        agg_exprs.append(F.avg(F.col(col_name)).alias(f"{col_name}_avg"))
    return df.agg(*agg_exprs)


def compute_aggregates_by_partition(df: DataFrame, numeric_cols: List[str], pt_keys: List[str]) -> DataFrame:
    """Compute count, sum, max, min, avg for each numeric column grouped by pt partition."""
    df_filtered = df.filter(F.col("pt").isin(pt_keys))
    agg_exprs = [F.count(F.lit(1)).alias("total_count")]
    for col_name in numeric_cols:
        agg_exprs.append(F.sum(F.col(col_name)).alias(f"{col_name}_sum"))
        agg_exprs.append(F.max(F.col(col_name)).alias(f"{col_name}_max"))
        agg_exprs.append(F.min(F.col(col_name)).alias(f"{col_name}_min"))
        agg_exprs.append(F.avg(F.col(col_name)).alias(f"{col_name}_avg"))
    return df_filtered.groupBy("pt").agg(*agg_exprs).orderBy("pt")


def compare_aggregates_non_partitioned(df_dbx: DataFrame, df_emr: DataFrame,
                                        numeric_cols: List[str], col_types: Dict[str, str],
                                        table_name: str) -> Dict:
    """Compare aggregate stats for a non-partitioned table."""
    dbx_agg = compute_aggregates(df_dbx, numeric_cols).collect()[0].asDict()
    emr_agg = compute_aggregates(df_emr, numeric_cols).collect()[0].asDict()

    diffs = []
    all_match = True

    if dbx_agg["total_count"] != emr_agg["total_count"]:
        diffs.append({
            "column": "*",
            "metric": "count",
            "delta_value": dbx_agg["total_count"],
            "iceberg_value": emr_agg["total_count"],
        })
        all_match = False

    for col_name in numeric_cols:
        for metric in ["sum", "max", "min", "avg"]:
            key = f"{col_name}_{metric}"
            dbx_val = dbx_agg.get(key)
            emr_val = emr_agg.get(key)
            if dbx_val != emr_val:
                if metric == "avg" and dbx_val is not None and emr_val is not None:
                    if abs(float(dbx_val) - float(emr_val)) < 1e-6:
                        continue
                diffs.append({
                    "column": col_name,
                    "metric": metric,
                    "delta_value": dbx_val,
                    "iceberg_value": emr_val,
                })
                all_match = False

    sql = build_agg_sql(table_name, numeric_cols)

    return {
        "table": table_name,
        "partitioned": False,
        "dbx_count": dbx_agg["total_count"],
        "emr_count": emr_agg["total_count"],
        "numeric_cols": numeric_cols,
        "col_types": col_types,
        "dbx_agg": dbx_agg,
        "emr_agg": emr_agg,
        "sql": sql,
        "diffs": diffs,
        "match": all_match,
    }


def compare_aggregates_partitioned(df_dbx: DataFrame, df_emr: DataFrame,
                                    numeric_cols: List[str], col_types: Dict[str, str],
                                    pt_keys: List[str],
                                    table_name: str) -> Dict:
    """Compare aggregate stats for a partitioned table, grouped by pt."""
    dbx_agg_df = compute_aggregates_by_partition(df_dbx, numeric_cols, pt_keys)
    emr_agg_df = compute_aggregates_by_partition(df_emr, numeric_cols, pt_keys)

    dbx_rows = {row["pt"]: row.asDict() for row in dbx_agg_df.collect()}
    emr_rows = {row["pt"]: row.asDict() for row in emr_agg_df.collect()}

    all_pts = sorted(set(list(dbx_rows.keys()) + list(emr_rows.keys())))
    partition_results = []
    all_match = True

    for pt_val in all_pts:
        dbx_data = dbx_rows.get(pt_val, {})
        emr_data = emr_rows.get(pt_val, {})
        pt_diffs = []

        if not dbx_data:
            pt_diffs.append({"column": "*", "metric": "partition", "delta_value": "MISSING", "iceberg_value": "EXISTS"})
            all_match = False
        elif not emr_data:
            pt_diffs.append({"column": "*", "metric": "partition", "delta_value": "EXISTS", "iceberg_value": "MISSING"})
            all_match = False
        else:
            if dbx_data.get("total_count") != emr_data.get("total_count"):
                pt_diffs.append({
                    "column": "*",
                    "metric": "count",
                    "delta_value": dbx_data.get("total_count"),
                    "iceberg_value": emr_data.get("total_count"),
                })
                all_match = False

            for col_name in numeric_cols:
                for metric in ["sum", "max", "min", "avg"]:
                    key = f"{col_name}_{metric}"
                    dbx_val = dbx_data.get(key)
                    emr_val = emr_data.get(key)
                    if dbx_val != emr_val:
                        if metric == "avg" and dbx_val is not None and emr_val is not None:
                            if abs(float(dbx_val) - float(emr_val)) < 1e-6:
                                continue
                        pt_diffs.append({
                            "column": col_name,
                            "metric": metric,
                            "delta_value": dbx_val,
                            "iceberg_value": emr_val,
                        })
                        all_match = False

        partition_results.append({
            "pt": pt_val,
            "dbx_data": dbx_data,
            "emr_data": emr_data,
            "dbx_count": dbx_data.get("total_count", 0),
            "emr_count": emr_data.get("total_count", 0),
            "diffs": pt_diffs,
            "match": len(pt_diffs) == 0,
        })

    sql = build_agg_sql(table_name, numeric_cols, pt_keys)

    return {
        "table": table_name,
        "partitioned": True,
        "pt_keys": pt_keys,
        "numeric_cols": numeric_cols,
        "col_types": col_types,
        "sql": sql,
        "partition_results": partition_results,
        "match": all_match,
    }


def format_non_partitioned_result_md(result: Dict) -> str:
    """Format non-partitioned aggregate comparison result as Markdown."""
    status = "PASS" if result['match'] else "FAIL"
    md = f"\n### {result['table']} — Aggregate Check: **{status}**\n\n"

    # SQL
    md += f"**SQL:**\n```sql\n{result['sql']}\n```\n\n"

    # Full results from both engines
    numeric_cols = result['numeric_cols']
    dbx_agg = result['dbx_agg']
    emr_agg = result['emr_agg']

    col_types = result.get('col_types', {})

    md += "**Results:**\n\n"
    md += "| Column | Type | Metric | Delta | Iceberg | Match |\n"
    md += "|--------|------|--------|-------|---------|-------|\n"
    count_match = dbx_agg["total_count"] == emr_agg["total_count"]
    md += f"| * | - | count | {dbx_agg['total_count']} | {emr_agg['total_count']} | {'Y' if count_match else 'N'} |\n"
    for col_name in numeric_cols:
        col_type = col_types.get(col_name, "")
        for metric in ["sum", "max", "min", "avg"]:
            key = f"{col_name}_{metric}"
            dbx_val = dbx_agg.get(key)
            emr_val = emr_agg.get(key)
            match = dbx_val == emr_val
            if metric == "avg" and dbx_val is not None and emr_val is not None:
                match = abs(float(dbx_val) - float(emr_val)) < 1e-6
            md += f"| {col_name} | {col_type} | {metric} | {dbx_val} | {emr_val} | {'Y' if match else 'N'} |\n"

    # Conclusion
    md += f"\n**Conclusion: {status}**\n"
    if not result['match']:
        md += "\nDifferences:\n"
        for diff in result['diffs']:
            md += f"- `{diff['column']}`.{diff['metric']}: Delta={diff['delta_value']}, Iceberg={diff['iceberg_value']}\n"

    return md


def format_partitioned_result_md(result: Dict) -> str:
    """Format partitioned aggregate comparison result as Markdown."""
    status = "PASS" if result['match'] else "FAIL"
    md = f"\n### {result['table']} — Partitioned Aggregate Check: **{status}**\n\n"

    # SQL
    md += f"**SQL:**\n```sql\n{result['sql']}\n```\n\n"

    numeric_cols = result['numeric_cols']
    col_types = result.get('col_types', {})

    for pr in result['partition_results']:
        pt_status = "PASS" if pr['match'] else "FAIL"
        md += f"\n#### Partition pt={pr['pt']} — **{pt_status}**\n\n"

        dbx_data = pr.get('dbx_data', {})
        emr_data = pr.get('emr_data', {})

        md += "| Column | Type | Metric | Delta | Iceberg | Match |\n"
        md += "|--------|------|--------|-------|---------|-------|\n"
        dbx_count = dbx_data.get("total_count", 0)
        emr_count = emr_data.get("total_count", 0)
        count_match = dbx_count == emr_count
        md += f"| * | - | count | {dbx_count} | {emr_count} | {'Y' if count_match else 'N'} |\n"
        for col_name in numeric_cols:
            col_type = col_types.get(col_name, "")
            for metric in ["sum", "max", "min", "avg"]:
                key = f"{col_name}_{metric}"
                dbx_val = dbx_data.get(key)
                emr_val = emr_data.get(key)
                match = dbx_val == emr_val
                if metric == "avg" and dbx_val is not None and emr_val is not None:
                    match = abs(float(dbx_val) - float(emr_val)) < 1e-6
                md += f"| {col_name} | {col_type} | {metric} | {dbx_val} | {emr_val} | {'Y' if match else 'N'} |\n"

        if pr['diffs']:
            md += f"\nDifferences:\n"
            for diff in pr['diffs']:
                md += f"- `{diff['column']}`.{diff['metric']}: Delta={diff['delta_value']}, Iceberg={diff['iceberg_value']}\n"

    md += f"\n**Conclusion: {status}**\n"

    return md


def compare_single_table(spark: SparkSession, config: Dict, report_path: str,
                         table_metadata: Dict = None):
    """Compare a single table between Databricks (Delta) and EMR (Iceberg).

    Logic:
    - Determine if partitioned by checking if 'pt' is in partition_cols (from Delta metadata)
    - Non-partitioned: compute count, sum, max, min, avg on numeric columns for both sides
    - Partitioned: filter by pt_keys, group by pt, compute count, sum, max, min, avg
    """
    table_name = config['table_name']
    pt_keys = [k.strip() for k in config['pt_keys'].split(',') if k.strip()] if config['pt_keys'] else []

    meta = table_metadata or {}
    part_cols = meta.get("partition_cols", [])
    is_partitioned = "pt" in part_cols

    start_time = time.time()
    md_content = f"\n---\n## Table: `{table_name}`\n"
    md_content += f"- Partitioned: {'Yes (pt)' if is_partitioned else 'No'}\n"
    if is_partitioned and pt_keys:
        md_content += f"- pt_keys: `{pt_keys}`\n"
    md_content += f"- Started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    append_md(report_path, md_content)

    try:
        dbx_location = meta.get("location", "")
        if not dbx_location:
            if meta.get("error"):
                raise Exception(f"Pre-fetch failed: {meta['error']}")
            raise Exception(f"No storage_location found for table {table_name}")

        df_dbx = read_delta_table(spark, dbx_location)
        df_emr = read_emr_table(spark, table_name)

        # Identify numeric columns from Delta table schema
        numeric_cols = identify_numeric_columns(df_dbx)

        if not numeric_cols:
            # No numeric columns — just compare count
            dbx_count = df_dbx.count()
            emr_count = df_emr.count()
            result_md = f"\n### {table_name} — Count Only (no numeric columns)\n\n"
            result_md += f"| Side | Count |\n|------|-------|\n"
            result_md += f"| Delta | {dbx_count} |\n"
            result_md += f"| Iceberg | {emr_count} |\n"
            if dbx_count != emr_count:
                result_md += f"\n> **FAIL** — Difference: {dbx_count - emr_count:+d}\n"
            else:
                result_md += f"\n> **PASS**\n"
            append_md(report_path, result_md)
        elif not is_partitioned:
            # Non-partitioned: full aggregate comparison
            col_types = get_column_types(df_dbx, numeric_cols)
            result = compare_aggregates_non_partitioned(df_dbx, df_emr, numeric_cols, col_types, table_name)
            append_md(report_path, format_non_partitioned_result_md(result))
        else:
            # Partitioned: aggregate by pt
            col_types = get_column_types(df_dbx, numeric_cols)
            if not pt_keys:
                dbx_pts = [row["pt"] for row in df_dbx.select("pt").distinct().collect()]
                emr_pts = [row["pt"] for row in df_emr.select("pt").distinct().collect()]
                pt_keys = sorted(set(dbx_pts + emr_pts))

            result = compare_aggregates_partitioned(df_dbx, df_emr, numeric_cols, col_types, pt_keys, table_name)
            append_md(report_path, format_partitioned_result_md(result))

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
    parser = argparse.ArgumentParser(description="Databricks vs EMR Data Diff Tool (v3 - Aggregate)")
    parser.add_argument("--csv", required=True, help="Path to input CSV file (space-delimited)")
    parser.add_argument("--s3-output", required=True, help="S3 path for output report")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help=f"Max parallel workers (default: {MAX_WORKERS})")
    parser.add_argument("--timeout", type=int, default=TIMEOUT_PER_TABLE, help=f"Timeout per table in seconds (default: {TIMEOUT_PER_TABLE})")
    parser.add_argument("--databricks-host", default=None, help="Databricks workspace URL")
    parser.add_argument("--databricks-token", default=None, help="Databricks access token")
    parser.add_argument("--databricks-secret-arn", default=None, help="AWS Secrets Manager ARN for OAuth2")
    parser.add_argument("--region", default="us-west-2", help="AWS region for Secrets Manager")
    args = parser.parse_args()

    global DATABRICKS_HOST, DATABRICKS_TOKEN
    if args.databricks_host:
        DATABRICKS_HOST = args.databricks_host

    if args.databricks_secret_arn:
        if not DATABRICKS_HOST:
            print("ERROR: --databricks-host is required when using --databricks-secret-arn", file=sys.stderr)
            sys.exit(1)
        client_id, client_secret = get_secret_from_sm(args.databricks_secret_arn, args.region)
        DATABRICKS_TOKEN = get_oauth2_token(DATABRICKS_HOST, client_id, client_secret)
    elif args.databricks_token:
        DATABRICKS_TOKEN = args.databricks_token

    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("ERROR: Databricks host and credentials must be provided", file=sys.stderr)
        sys.exit(1)

    spark = get_spark()

    configs = parse_csv(args.csv)
    print(f"Loaded {len(configs)} table configs from {args.csv}")

    prefetch_start = time.time()
    all_metadata = prefetch_all_table_metadata(configs)
    prefetch_elapsed = time.time() - prefetch_start
    print(f"Metadata pre-fetch took {prefetch_elapsed:.1f}s")

    report_path = tempfile.mktemp(suffix=".md")
    header = f"# Databricks vs EMR Data Diff Report (v3 - Aggregate)\n\n"
    header += f"- Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    header += f"- Tables: {len(configs)}\n"
    header += f"- Workers: {args.workers}\n"
    header += f"- Metadata pre-fetch: {prefetch_elapsed:.1f}s\n\n"
    with open(report_path, 'w') as f:
        f.write(header)

    total_start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for config in configs:
            table_meta = all_metadata.get(config['table_name'], {})
            future = executor.submit(compare_single_table, spark, config, report_path, table_meta)
            futures[future] = config['table_name']

        for future in as_completed(futures):
            table = futures[future]
            try:
                future.result(timeout=args.timeout)
                print(f"DONE: {table}")
            except Exception as e:
                print(f"TIMEOUT/ERROR: {table}: {e}", file=sys.stderr)
                error_md = f"\n---\n## Table: `{table}` — **TIMEOUT**\n\n```\n{str(e)}\n```\n"
                append_md(report_path, error_md)

    total_elapsed = time.time() - total_start
    summary = f"\n---\n## Summary\n\n"
    summary += f"- Total tables: {len(configs)}\n"
    summary += f"- Metadata pre-fetch: {prefetch_elapsed:.1f}s\n"
    summary += f"- Comparison time: {total_elapsed:.1f}s\n"
    summary += f"- Total time: {prefetch_elapsed + total_elapsed:.1f}s\n"
    append_md(report_path, summary)

    # Print report to driver log (stdout)
    with open(report_path, 'r') as f:
        report_content = f.read()
    print("\n" + "=" * 80)
    print("REPORT OUTPUT:")
    print("=" * 80)
    print(report_content)
    print("=" * 80 + "\n")

    upload_to_s3(report_path, args.s3_output)
    os.remove(report_path)
    spark.stop()
    print(f"Done. Report uploaded to {args.s3_output}")


if __name__ == "__main__":
    main()
