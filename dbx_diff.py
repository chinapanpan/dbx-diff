"""
Databricks vs EMR Data Comparison Tool (v5 - Scheduler Edition).

Compares numeric column aggregates (count, sum, max, min) between
Databricks (Delta on S3) and EMR (Iceberg via Glue catalog).
Results are written to an Iceberg table instead of S3.

All parameters are passed via --widget as a JSON string.
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
import json
import threading
import argparse
import requests
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    IntegerType, LongType, FloatType, DoubleType,
    DecimalType, ShortType, ByteType,
    StructType, StructField, StringType, ArrayType
)
from pyspark.sql import functions as F


DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
MAX_WORKERS = 15
TIMEOUT_PER_TABLE = 1800

_token_lock = threading.Lock()
_oauth2_token = None
_oauth2_expiry = 0

NUMERIC_TYPES = (IntegerType, LongType, FloatType, DoubleType, DecimalType, ShortType, ByteType)


def get_spark() -> SparkSession:
    """Get or create SparkSession."""
    return SparkSession.builder \
        .appName("DbxDiff") \
        .getOrCreate()


def get_secret_from_sm(secret_arn: str, region: str = "us-west-2") -> str:
    """Retrieve secret value from AWS Secrets Manager."""
    import boto3
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_arn)
    secret_str = resp["SecretString"].strip()
    print(f"Retrieved credentials from Secrets Manager: {secret_arn}")
    return secret_str


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
    """Fetch table metadata from Databricks Unity Catalog API."""
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


def read_delta_table(spark: SparkSession, s3_location: str) -> DataFrame:
    """Read a Delta table from S3 location."""
    return spark.read.format("delta").load(s3_location)


def read_emr_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a table from EMR Spark catalog (Iceberg via Glue)."""
    return spark.table(table_name)


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


def build_agg_sql(table_name: str, numeric_cols: List[str], pt_start: str = None, pt_end: str = None) -> str:
    """Build the SQL string for aggregate comparison (for logging)."""
    agg_parts = ["count(1) AS total_count"]
    for col_name in numeric_cols:
        agg_parts.append(f"sum({col_name}) AS {col_name}_sum")
        agg_parts.append(f"max({col_name}) AS {col_name}_max")
        agg_parts.append(f"min({col_name}) AS {col_name}_min")
    select_clause = ",\n       ".join(agg_parts)

    if pt_start and pt_end:
        pt_end_plus1 = str(int(pt_end) + 1)
        return (f"SELECT pt,\n       {select_clause}\n"
                f"FROM {table_name}\n"
                f"WHERE pt >= '{pt_start}' AND pt < '{pt_end_plus1}'\n"
                f"GROUP BY pt\nORDER BY pt")
    else:
        return f"SELECT {select_clause}\nFROM {table_name}"


def compute_aggregates(df: DataFrame, numeric_cols: List[str]) -> DataFrame:
    """Compute count, sum, max, min for each numeric column."""
    agg_exprs = [F.count(F.lit(1)).alias("total_count")]
    for col_name in numeric_cols:
        agg_exprs.append(F.sum(F.col(col_name)).alias(f"{col_name}_sum"))
        agg_exprs.append(F.max(F.col(col_name)).alias(f"{col_name}_max"))
        agg_exprs.append(F.min(F.col(col_name)).alias(f"{col_name}_min"))
    return df.agg(*agg_exprs)


def compute_aggregates_by_partition(df: DataFrame, numeric_cols: List[str], pt_start: str, pt_end: str) -> DataFrame:
    """Compute aggregates grouped by pt partition."""
    pt_end_plus1 = str(int(pt_end) + 1)
    df_filtered = df.filter((F.col("pt") >= pt_start) & (F.col("pt") < pt_end_plus1))
    agg_exprs = [F.count(F.lit(1)).alias("total_count")]
    for col_name in numeric_cols:
        agg_exprs.append(F.sum(F.col(col_name)).alias(f"{col_name}_sum"))
        agg_exprs.append(F.max(F.col(col_name)).alias(f"{col_name}_max"))
        agg_exprs.append(F.min(F.col(col_name)).alias(f"{col_name}_min"))
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
            "col_type": "-",
            "metric": "count",
            "delta_value": str(dbx_agg["total_count"]),
            "iceberg_value": str(emr_agg["total_count"]),
        })
        all_match = False

    for col_name in numeric_cols:
        for metric in ["sum", "max", "min"]:
            key = f"{col_name}_{metric}"
            dbx_val = dbx_agg.get(key)
            emr_val = emr_agg.get(key)
            if dbx_val != emr_val:
                diffs.append({
                    "column": col_name,
                    "col_type": col_types.get(col_name, ""),
                    "metric": metric,
                    "delta_value": str(dbx_val),
                    "iceberg_value": str(emr_val),
                })
                all_match = False

    sql = build_agg_sql(table_name, numeric_cols)
    return {
        "table": table_name,
        "partitioned": False,
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
                                    pt_start: str, pt_end: str,
                                    table_name: str) -> Dict:
    """Compare aggregate stats for a partitioned table, grouped by pt."""
    dbx_agg_df = compute_aggregates_by_partition(df_dbx, numeric_cols, pt_start, pt_end)
    emr_agg_df = compute_aggregates_by_partition(df_emr, numeric_cols, pt_start, pt_end)

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
            pt_diffs.append({"column": "*", "col_type": "-", "metric": "count",
                             "delta_value": "MISSING", "iceberg_value": "EXISTS"})
            all_match = False
        elif not emr_data:
            pt_diffs.append({"column": "*", "col_type": "-", "metric": "count",
                             "delta_value": "EXISTS", "iceberg_value": "MISSING"})
            all_match = False
        else:
            if dbx_data.get("total_count") != emr_data.get("total_count"):
                pt_diffs.append({
                    "column": "*",
                    "col_type": "-",
                    "metric": "count",
                    "delta_value": str(dbx_data.get("total_count")),
                    "iceberg_value": str(emr_data.get("total_count")),
                })
                all_match = False

            for col_name in numeric_cols:
                for metric in ["sum", "max", "min"]:
                    key = f"{col_name}_{metric}"
                    dbx_val = dbx_data.get(key)
                    emr_val = emr_data.get(key)
                    if dbx_val != emr_val:
                        pt_diffs.append({
                            "column": col_name,
                            "col_type": col_types.get(col_name, ""),
                            "metric": metric,
                            "delta_value": str(dbx_val),
                            "iceberg_value": str(emr_val),
                        })
                        all_match = False

        partition_results.append({
            "pt": pt_val,
            "dbx_data": dbx_data,
            "emr_data": emr_data,
            "diffs": pt_diffs,
            "match": len(pt_diffs) == 0,
        })

    sql = build_agg_sql(table_name, numeric_cols, pt_start, pt_end)
    return {
        "table": table_name,
        "partitioned": True,
        "pt_start": pt_start,
        "pt_end": pt_end,
        "numeric_cols": numeric_cols,
        "col_types": col_types,
        "sql": sql,
        "partition_results": partition_results,
        "match": all_match,
    }


def log_non_partitioned_result(result: Dict):
    """Print non-partitioned comparison result to driver log."""
    status = "PASS" if result['match'] else "FAIL"
    print(f"\n### {result['table']} — Aggregate Check: **{status}**")
    print(f"SQL: {result['sql']}")

    numeric_cols = result['numeric_cols']
    dbx_agg = result['dbx_agg']
    emr_agg = result['emr_agg']
    col_types = result.get('col_types', {})

    print(f"| {'Column':<15} | {'Type':<10} | {'Metric':<7} | {'Delta':<15} | {'Iceberg':<15} | Match |")
    print(f"|{'-'*17}|{'-'*12}|{'-'*9}|{'-'*17}|{'-'*17}|-------|")
    count_match = dbx_agg["total_count"] == emr_agg["total_count"]
    print(f"| {'*':<15} | {'-':<10} | {'count':<7} | {str(dbx_agg['total_count']):<15} | {str(emr_agg['total_count']):<15} | {'Y' if count_match else 'N'}     |")
    for col_name in numeric_cols:
        col_type = col_types.get(col_name, "")
        for metric in ["sum", "max", "min"]:
            key = f"{col_name}_{metric}"
            dbx_val = dbx_agg.get(key)
            emr_val = emr_agg.get(key)
            match = dbx_val == emr_val
            print(f"| {col_name:<15} | {col_type:<10} | {metric:<7} | {str(dbx_val):<15} | {str(emr_val):<15} | {'Y' if match else 'N'}     |")

    if not result['match']:
        print("Differences:")
        for diff in result['diffs']:
            print(f"  - `{diff['column']}`.{diff['metric']}: Delta={diff['delta_value']}, Iceberg={diff['iceberg_value']}")


def log_partitioned_result(result: Dict):
    """Print partitioned comparison result to driver log."""
    status = "PASS" if result['match'] else "FAIL"
    print(f"\n### {result['table']} — Partitioned Aggregate Check: **{status}**")
    print(f"SQL: {result['sql']}")

    numeric_cols = result['numeric_cols']
    col_types = result.get('col_types', {})

    for pr in result['partition_results']:
        pt_status = "PASS" if pr['match'] else "FAIL"
        print(f"\n  Partition pt={pr['pt']} — {pt_status}")
        dbx_data = pr.get('dbx_data', {})
        emr_data = pr.get('emr_data', {})

        dbx_count = dbx_data.get("total_count", 0)
        emr_count = emr_data.get("total_count", 0)
        count_match = dbx_count == emr_count
        print(f"    count: Delta={dbx_count}, Iceberg={emr_count}, Match={'Y' if count_match else 'N'}")
        for col_name in numeric_cols:
            col_type = col_types.get(col_name, "")
            for metric in ["sum", "max", "min"]:
                key = f"{col_name}_{metric}"
                dbx_val = dbx_data.get(key)
                emr_val = emr_data.get(key)
                match = dbx_val == emr_val
                print(f"    {col_name}({col_type}).{metric}: Delta={dbx_val}, Iceberg={emr_val}, Match={'Y' if match else 'N'}")

        if pr['diffs']:
            for diff in pr['diffs']:
                print(f"    DIFF: `{diff['column']}`.{diff['metric']}: Delta={diff['delta_value']}, Iceberg={diff['iceberg_value']}")


def _detail_row(field_name, field_type, task_type, delta_value, iceberg_value, result):
    """Create a detail tuple matching the struct schema order."""
    return (field_name, field_type, task_type, delta_value, iceberg_value, result)


def build_iceberg_rows_non_partitioned(result: Dict, task_id: str, instance_id: str, attemp_id: str) -> List[tuple]:
    """Build Iceberg output rows for a non-partitioned table result."""
    table_name = result['table']
    numeric_cols = result.get('numeric_cols', [])
    col_types = result.get('col_types', {})
    dbx_agg = result.get('dbx_agg', {})
    emr_agg = result.get('emr_agg', {})

    details = []
    count_match = dbx_agg.get("total_count") == emr_agg.get("total_count")
    details.append(_detail_row("*", "-", "count",
                               str(dbx_agg.get("total_count", "")),
                               str(emr_agg.get("total_count", "")),
                               "Y" if count_match else "N"))

    for col_name in numeric_cols:
        col_type = col_types.get(col_name, "")
        for metric in ["sum", "max", "min"]:
            key = f"{col_name}_{metric}"
            dbx_val = dbx_agg.get(key)
            emr_val = emr_agg.get(key)
            match = dbx_val == emr_val
            details.append(_detail_row(col_name, col_type, metric,
                                       str(dbx_val) if dbx_val is not None else "",
                                       str(emr_val) if emr_val is not None else "",
                                       "Y" if match else "N"))

    overall_result = "Y" if result.get('match', False) else "N"
    return [(task_id, instance_id, attemp_id, table_name, 0, overall_result, details)]


def build_iceberg_rows_partitioned(result: Dict, task_id: str, instance_id: str, attemp_id: str) -> List[tuple]:
    """Build Iceberg output rows for a partitioned table result (one row per partition)."""
    table_name = result['table']
    numeric_cols = result.get('numeric_cols', [])
    col_types = result.get('col_types', {})
    rows = []

    for pr in result.get('partition_results', []):
        pt_val = pr['pt']
        dbx_data = pr.get('dbx_data', {})
        emr_data = pr.get('emr_data', {})

        details = []
        dbx_count = dbx_data.get("total_count", 0)
        emr_count = emr_data.get("total_count", 0)
        count_match = dbx_count == emr_count
        details.append(_detail_row("*", "-", "count",
                                   str(dbx_count), str(emr_count),
                                   "Y" if count_match else "N"))

        for col_name in numeric_cols:
            col_type = col_types.get(col_name, "")
            for metric in ["sum", "max", "min"]:
                key = f"{col_name}_{metric}"
                dbx_val = dbx_data.get(key)
                emr_val = emr_data.get(key)
                match = dbx_val == emr_val
                details.append(_detail_row(col_name, col_type, metric,
                                           str(dbx_val) if dbx_val is not None else "",
                                           str(emr_val) if emr_val is not None else "",
                                           "Y" if match else "N"))

        pt_result = "Y" if pr.get('match', False) else "N"
        try:
            pt_long = int(pt_val)
        except (ValueError, TypeError):
            pt_long = 0

        rows.append((task_id, instance_id, attemp_id, table_name, pt_long, pt_result, details))

    return rows


def build_iceberg_rows_count_only(result: Dict, task_id: str, instance_id: str, attemp_id: str) -> List[tuple]:
    """Build Iceberg output rows for count-only comparison."""
    table_name = result['table']
    is_partitioned = result.get('partitioned', False)
    rows = []

    if is_partitioned and result.get('partition_counts'):
        for pt_info in result['partition_counts']:
            pt_val = pt_info['pt']
            dbx_count = pt_info['delta_count']
            emr_count = pt_info['iceberg_count']
            match = dbx_count == emr_count
            details = [_detail_row("*", "-", "count",
                                   str(dbx_count), str(emr_count),
                                   "Y" if match else "N")]
            try:
                pt_long = int(pt_val)
            except (ValueError, TypeError):
                pt_long = 0
            rows.append((task_id, instance_id, attemp_id, table_name, pt_long,
                        "Y" if match else "N", details))
    else:
        dbx_count = result.get('dbx_count', 0)
        emr_count = result.get('emr_count', 0)
        match = dbx_count == emr_count
        details = [_detail_row("*", "-", "count",
                               str(dbx_count), str(emr_count),
                               "Y" if match else "N")]
        rows.append((task_id, instance_id, attemp_id, table_name, 0,
                    "Y" if match else "N", details))

    return rows


def write_results_to_iceberg(spark: SparkSession, iceberg_table: str, all_rows: List[tuple]):
    """Write comparison results to Iceberg table."""
    if not all_rows:
        print("No results to write to Iceberg table.")
        return

    detail_schema = ArrayType(StructType([
        StructField("field_name", StringType(), True),
        StructField("field_type", StringType(), True),
        StructField("task_type", StringType(), True),
        StructField("delta_value", StringType(), True),
        StructField("iceberg_value", StringType(), True),
        StructField("result", StringType(), True),
    ]))

    schema = StructType([
        StructField("task_id", StringType(), True),
        StructField("instance_id", StringType(), True),
        StructField("attemp_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("pt", LongType(), True),
        StructField("result", StringType(), True),
        StructField("details", detail_schema, True),
    ])

    df = spark.createDataFrame(all_rows, schema)
    df.writeTo(iceberg_table).append()
    print(f"Successfully wrote {len(all_rows)} rows to Iceberg table: {iceberg_table}")


def compare_single_table(spark: SparkSession, table_name: str,
                         table_metadata: Dict = None, pt_start: str = None, pt_end: str = None) -> Dict:
    """Compare a single table between Databricks (Delta) and EMR (Iceberg)."""
    meta = table_metadata or {}
    part_cols = meta.get("partition_cols", [])
    is_partitioned = "pt" in part_cols

    start_time = time.time()
    print(f"\n--- Table: {table_name} ---")
    print(f"  Partitioned: {'Yes (pt)' if is_partitioned else 'No'}")
    if is_partitioned and pt_start and pt_end:
        print(f"  Partition range: pt >= {pt_start} AND pt < {int(pt_end) + 1}")

    try:
        dbx_location = meta.get("location", "")
        if not dbx_location:
            if meta.get("error"):
                raise Exception(f"Pre-fetch failed: {meta['error']}")
            raise Exception(f"No storage_location found for table {table_name}")

        df_dbx = read_delta_table(spark, dbx_location)
        df_emr = read_emr_table(spark, table_name)

        numeric_cols = identify_numeric_columns(df_dbx)

        if not numeric_cols:
            if is_partitioned and pt_start and pt_end:
                pt_end_plus1 = str(int(pt_end) + 1)
                df_dbx_filtered = df_dbx.filter((F.col("pt") >= pt_start) & (F.col("pt") < pt_end_plus1))
                df_emr_filtered = df_emr.filter((F.col("pt") >= pt_start) & (F.col("pt") < pt_end_plus1))
            elif is_partitioned:
                df_dbx_filtered = df_dbx
                df_emr_filtered = df_emr
            else:
                df_dbx_filtered = df_dbx
                df_emr_filtered = df_emr

            if is_partitioned:
                dbx_pt_counts = {row["pt"]: row["cnt"] for row in
                                 df_dbx_filtered.groupBy("pt").agg(F.count(F.lit(1)).alias("cnt")).collect()}
                emr_pt_counts = {row["pt"]: row["cnt"] for row in
                                 df_emr_filtered.groupBy("pt").agg(F.count(F.lit(1)).alias("cnt")).collect()}
                all_pts = sorted(set(list(dbx_pt_counts.keys()) + list(emr_pt_counts.keys())))
                all_match = True
                partition_counts = []
                print(f"  Count-only (no numeric columns), partitioned:")
                for pt_val in all_pts:
                    d_cnt = dbx_pt_counts.get(pt_val, 0)
                    e_cnt = emr_pt_counts.get(pt_val, 0)
                    pt_match = d_cnt == e_cnt
                    print(f"    pt={pt_val}: Delta={d_cnt}, Iceberg={e_cnt}, Match={'Y' if pt_match else 'N'}")
                    if not pt_match:
                        all_match = False
                    partition_counts.append({"pt": pt_val, "delta_count": d_cnt, "iceberg_count": e_cnt})
                elapsed = time.time() - start_time
                print(f"  Completed in {elapsed:.1f}s — {'PASS' if all_match else 'FAIL'}")
                return {"table": table_name, "match": all_match, "partitioned": True,
                        "partition_counts": partition_counts, "count_only": True}
            else:
                dbx_count = df_dbx_filtered.count()
                emr_count = df_emr_filtered.count()
                match = dbx_count == emr_count
                print(f"  Count-only (no numeric columns): Delta={dbx_count}, Iceberg={emr_count}, Match={'Y' if match else 'N'}")
                elapsed = time.time() - start_time
                print(f"  Completed in {elapsed:.1f}s — {'PASS' if match else 'FAIL'}")
                return {"table": table_name, "match": match, "partitioned": False,
                        "dbx_count": dbx_count, "emr_count": emr_count, "count_only": True}

        elif not is_partitioned:
            col_types = get_column_types(df_dbx, numeric_cols)
            result = compare_aggregates_non_partitioned(df_dbx, df_emr, numeric_cols, col_types, table_name)
            log_non_partitioned_result(result)
            elapsed = time.time() - start_time
            print(f"  Completed in {elapsed:.1f}s")
            return result

        else:
            col_types = get_column_types(df_dbx, numeric_cols)
            if not pt_start or not pt_end:
                dbx_pts = [row["pt"] for row in df_dbx.select("pt").distinct().collect()]
                emr_pts = [row["pt"] for row in df_emr.select("pt").distinct().collect()]
                all_pts = sorted(set(dbx_pts + emr_pts))
                if all_pts:
                    pt_start = pt_start or str(all_pts[0])
                    pt_end = pt_end or str(all_pts[-1])
                else:
                    print(f"  No partitions found — PASS")
                    elapsed = time.time() - start_time
                    print(f"  Completed in {elapsed:.1f}s")
                    return {"table": table_name, "match": True, "partitioned": True, "partition_results": []}

            result = compare_aggregates_partitioned(df_dbx, df_emr, numeric_cols, col_types, pt_start, pt_end, table_name)
            log_partitioned_result(result)
            elapsed = time.time() - start_time
            print(f"  Completed in {elapsed:.1f}s")
            return result

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"  ERROR for {table_name}: {e}")
        print(f"  Failed after {elapsed:.1f}s")
        return {"table": table_name, "match": False, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Databricks vs EMR Data Diff Tool (v5 - Scheduler)")
    parser.add_argument("--widget", required=True, help="JSON string with all parameters")
    args = parser.parse_args()

    params = json.loads(args.widget)

    table_name = params.get("table-name")
    iceberg_output = params.get("iceberg-output")
    task_id = params.get("task-id")
    instance_id = params.get("instance-id")
    attemp_id = params.get("attemp-id")
    databricks_host = params.get("databricks-host")
    databricks_secret_arn = params.get("databricks-secret-arn")
    region = params.get("region", "us-west-2")
    pt_start = params.get("pt-start")
    pt_end = params.get("pt-end")
    workers = int(params.get("workers", MAX_WORKERS))
    timeout = int(params.get("timeout", TIMEOUT_PER_TABLE))

    required_params = {
        "table-name": table_name,
        "iceberg-output": iceberg_output,
        "databricks-host": databricks_host,
        "databricks-secret-arn": databricks_secret_arn,
        "pt-start": pt_start,
        "pt-end": pt_end,
        "task-id": task_id,
        "instance-id": instance_id,
        "attemp-id": attemp_id,
    }
    missing = [k for k, v in required_params.items() if not v]
    if missing:
        print(f"ERROR: Missing required params in --widget JSON: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    global DATABRICKS_HOST, DATABRICKS_TOKEN
    DATABRICKS_HOST = databricks_host

    secret_value = get_secret_from_sm(databricks_secret_arn, region)
    if ":" in secret_value and not secret_value.startswith("dapi"):
        client_id, client_secret = secret_value.split(":", 1)
        DATABRICKS_TOKEN = get_oauth2_token(DATABRICKS_HOST, client_id, client_secret)
    else:
        DATABRICKS_TOKEN = secret_value
        print("Using PAT token from Secrets Manager")

    spark = get_spark()

    print("=" * 80)
    print("Databricks vs EMR Data Diff (v5 - Scheduler)")
    print("=" * 80)
    print(f"  Table: {table_name}")
    print(f"  Iceberg output: {iceberg_output}")
    print(f"  Task ID: {task_id}")
    print(f"  Instance ID: {instance_id}")
    print(f"  Attemp ID: {attemp_id}")
    print(f"  Partition range: pt >= {pt_start} AND pt < {int(pt_end) + 1}")
    print(f"  Workers: {workers}, Timeout: {timeout}s")
    print("=" * 80)

    configs = [{'table_name': table_name}]

    prefetch_start = time.time()
    all_metadata = {}
    print(f"Pre-fetching metadata for {len(configs)} tables from Databricks API...")
    for config in configs:
        tname = config['table_name']
        try:
            meta = fetch_table_metadata(tname)
            all_metadata[tname] = meta
        except Exception as e:
            print(f"  WARNING: Failed to fetch metadata for {tname}: {e}", file=sys.stderr)
            all_metadata[tname] = {"location": "", "partition_cols": [], "error": str(e)}
    prefetch_elapsed = time.time() - prefetch_start
    print(f"Metadata pre-fetch took {prefetch_elapsed:.1f}s")

    results = []
    total_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for config in configs:
            table_meta = all_metadata.get(config['table_name'], {})
            future = executor.submit(compare_single_table, spark, config['table_name'],
                                     table_meta, pt_start, pt_end)
            futures[future] = config['table_name']

        for future in as_completed(futures):
            tname = futures[future]
            try:
                result = future.result(timeout=timeout)
                results.append(result)
                print(f"DONE: {tname}")
            except Exception as e:
                print(f"TIMEOUT/ERROR: {tname}: {e}", file=sys.stderr)
                results.append({"table": tname, "match": False, "error": str(e)})

    total_elapsed = time.time() - total_start
    print(f"\nTotal comparison time: {total_elapsed:.1f}s")

    all_iceberg_rows = []
    for result in results:
        if result.get("error"):
            error_details = [_detail_row("ERROR", "-", "count", result["error"][:500], "", "N")]
            all_iceberg_rows.append((task_id, instance_id, attemp_id, result["table"], 0, "N", error_details))
        elif result.get("count_only"):
            all_iceberg_rows.extend(build_iceberg_rows_count_only(result, task_id, instance_id, attemp_id))
        elif result.get("partitioned"):
            all_iceberg_rows.extend(build_iceberg_rows_partitioned(result, task_id, instance_id, attemp_id))
        else:
            all_iceberg_rows.extend(build_iceberg_rows_non_partitioned(result, task_id, instance_id, attemp_id))

    write_results_to_iceberg(spark, iceberg_output, all_iceberg_rows)

    passed = sum(1 for r in results if r.get("match", False))
    failed = len(results) - passed
    print(f"\n{'=' * 80}")
    print(f"SUMMARY: {passed} PASS, {failed} FAIL out of {len(results)} tables")
    print(f"{'=' * 80}")

    spark.stop()


if __name__ == "__main__":
    main()
