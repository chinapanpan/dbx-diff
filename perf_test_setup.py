"""
Performance test: create 50 tables in both Databricks and EMR to test concurrency.
Uses small data per table to focus on parallelism overhead, not data volume.
"""

import sys
import os
import time
import io
import csv
import requests

from pyspark.sql import SparkSession

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-51ad87e6-c26d.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
EMR_CATALOG = "iceberg_catalog"
EMR_DB = "demo2"
EMR_WAREHOUSE = "s3://zpf-databricks-event/emr/demo2"
NUM_TABLES = int(os.environ.get("NUM_TABLES", "50"))


def dbx_sql(sql: str) -> dict:
    url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    wh_url = f"{DATABRICKS_HOST}/api/2.0/sql/warehouses"
    wh_resp = requests.get(wh_url, headers=headers, timeout=30)
    warehouses = wh_resp.json().get("warehouses", [])
    if not warehouses:
        raise Exception("No SQL warehouse found")
    warehouse_id = warehouses[0]["id"]

    payload = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "50s",
        "catalog": "workspace",
        "schema": "demo2",
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    if resp.status_code != 200:
        raise Exception(f"Databricks SQL failed: {resp.status_code} {resp.text}")
    result = resp.json()
    status = result.get("status", {}).get("state", "")
    stmt_id = result.get("statement_id")
    while status in ("PENDING", "RUNNING"):
        time.sleep(2)
        poll_url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/{stmt_id}"
        poll_resp = requests.get(poll_url, headers=headers, timeout=30)
        result = poll_resp.json()
        status = result.get("status", {}).get("state", "")
    if status == "FAILED":
        raise Exception(f"SQL failed: {result.get('status', {}).get('error', {})}")
    return result


def setup_perf_tables():
    spark = SparkSession.builder \
        .appName("PerfTestSetup") \
        .config(f"spark.sql.catalog.{EMR_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{EMR_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{EMR_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config(f"spark.sql.catalog.{EMR_CATALOG}.warehouse", EMR_WAREHOUSE) \
        .getOrCreate()

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {EMR_CATALOG}.{EMR_DB}")

    csv_rows = []

    for i in range(NUM_TABLES):
        tbl = f"perf_test_{i:03d}"
        full_emr = f"{EMR_CATALOG}.{EMR_DB}.{tbl}"
        full_dbx = f"workspace.demo2.{tbl}"

        # Create EMR table
        spark.sql(f"DROP TABLE IF EXISTS {full_emr}")
        spark.sql(f"""
            CREATE TABLE {full_emr} (id INT, val INT, pt STRING)
            USING iceberg PARTITIONED BY (pt)
        """)
        spark.sql(f"""
            INSERT INTO {full_emr} VALUES
            (1, 100, '20250101'), (2, 200, '20250101'),
            (3, 300, '20250102'), (4, 400, '20250102')
        """)

        # Create Databricks table
        dbx_sql(f"DROP TABLE IF EXISTS {tbl}")
        dbx_sql(f"""
            CREATE TABLE {tbl} (id INT, val INT, pt STRING)
            USING DELTA PARTITIONED BY (pt)
        """)
        dbx_sql(f"""
            INSERT INTO {tbl} VALUES
            (1, 100, '20250101'), (2, 200, '20250101'),
            (3, 300, '20250102'), (4, 400, '20250102')
        """)

        # Introduce a diff in every 5th table
        if i % 5 == 0:
            dbx_sql(f"UPDATE {tbl} SET val = 999 WHERE id = 1 AND pt = '20250101'")

        csv_rows.append({
            'table_name': full_dbx,
            'primary_keys': 'id',
            'pt_start': '20250101',
            'pt_end': '20250102',
            'pt_keys': '20250101,20250102',
        })

        if (i + 1) % 10 == 0:
            print(f"  Created {i + 1}/{NUM_TABLES} tables")

    spark.stop()

    # Write space-delimited CSV
    csv_path = f"/home/hadoop/version2/tables_perf_{NUM_TABLES}.csv"
    with open(csv_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=['table_name', 'primary_keys', 'pt_start', 'pt_end', 'pt_keys'], delimiter=' ')
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)

    print(f"\nDone. Created {NUM_TABLES} tables. CSV written to {csv_path}")
    print(f"Tables with diffs: {[i for i in range(NUM_TABLES) if i % 5 == 0]}")


if __name__ == "__main__":
    setup_perf_tables()
