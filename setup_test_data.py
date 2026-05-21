"""
Set up test data in Databricks (workspace.demo2) and EMR (Spark/Glue catalog)
to cover all 4 comparison scenarios.

Test tables:
1. test_pk_nopart     — primary keys, non-partitioned (row-level diff)
2. test_pk_part       — primary keys, partitioned (partition count + row-level)
3. test_nopk_nopart   — no primary keys, non-partitioned (count only)
4. test_nopk_part     — no primary keys, partitioned (partition count + per-pt count)
"""

import sys
import os
import time
import json
import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-51ad87e6-c26d.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DBX_CATALOG = "workspace"
DBX_SCHEMA = "demo2"

EMR_CATALOG = "iceberg_catalog"
EMR_DB = "demo2"
EMR_WAREHOUSE = "s3://zpf-databricks-event/emr/demo2"


def dbx_sql(sql: str, wait: bool = True) -> dict:
    """Execute SQL on Databricks via SQL Statement API."""
    url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "statement": sql,
        "warehouse_id": None,
        "wait_timeout": "60s" if wait else "0s",
        "catalog": DBX_CATALOG,
        "schema": DBX_SCHEMA,
    }
    # First get a warehouse
    wh_url = f"{DATABRICKS_HOST}/api/2.0/sql/warehouses"
    wh_resp = requests.get(wh_url, headers=headers, timeout=30)
    warehouses = wh_resp.json().get("warehouses", [])
    if not warehouses:
        raise Exception("No SQL warehouse found in Databricks. Please create one.")
    warehouse_id = warehouses[0]["id"]
    payload["warehouse_id"] = warehouse_id

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    if resp.status_code != 200:
        raise Exception(f"Databricks SQL failed: {resp.status_code} {resp.text}")
    result = resp.json()
    status = result.get("status", {}).get("state", "")
    if status == "FAILED":
        raise Exception(f"SQL failed: {result.get('status', {}).get('error', {})}")
    # Poll if pending
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


def setup_databricks_tables():
    """Create and populate test tables in Databricks workspace.demo2."""
    print("Setting up Databricks tables...")

    # 1. test_pk_nopart — with primary keys, no partition
    dbx_sql("DROP TABLE IF EXISTS test_pk_nopart")
    dbx_sql("""
        CREATE TABLE test_pk_nopart (
            id INT, name STRING, value INT
        ) USING DELTA
    """)
    dbx_sql("""
        INSERT INTO test_pk_nopart VALUES
        (1, 'alice', 100),
        (2, 'bob', 200),
        (3, 'charlie', 300),
        (4, 'david', 400),
        (5, 'eve', 500)
    """)
    print("  Created test_pk_nopart")

    # 2. test_pk_part — with primary keys, partitioned by pt
    dbx_sql("DROP TABLE IF EXISTS test_pk_part")
    dbx_sql("""
        CREATE TABLE test_pk_part (
            id INT, name STRING, amount INT, pt STRING
        ) USING DELTA PARTITIONED BY (pt)
    """)
    dbx_sql("""
        INSERT INTO test_pk_part VALUES
        (1, 'alice', 10, '20250101'),
        (2, 'bob', 20, '20250101'),
        (3, 'charlie', 30, '20250102'),
        (4, 'david', 40, '20250102'),
        (5, 'eve', 50, '20250103')
    """)
    print("  Created test_pk_part")

    # 3. test_nopk_nopart — no primary keys, no partition
    dbx_sql("DROP TABLE IF EXISTS test_nopk_nopart")
    dbx_sql("""
        CREATE TABLE test_nopk_nopart (
            event STRING, ts STRING, payload STRING
        ) USING DELTA
    """)
    dbx_sql("""
        INSERT INTO test_nopk_nopart VALUES
        ('click', '2025-01-01 00:00:00', '{"page":"home"}'),
        ('view', '2025-01-01 00:01:00', '{"page":"about"}'),
        ('click', '2025-01-01 00:02:00', '{"page":"shop"}')
    """)
    print("  Created test_nopk_nopart")

    # 4. test_nopk_part — no primary keys, partitioned
    dbx_sql("DROP TABLE IF EXISTS test_nopk_part")
    dbx_sql("""
        CREATE TABLE test_nopk_part (
            event STRING, ts STRING, payload STRING, pt STRING
        ) USING DELTA PARTITIONED BY (pt)
    """)
    dbx_sql("""
        INSERT INTO test_nopk_part VALUES
        ('click', '2025-01-01 00:00:00', '{"p":"a"}', '20250101'),
        ('view', '2025-01-01 00:01:00', '{"p":"b"}', '20250101'),
        ('click', '2025-01-02 00:00:00', '{"p":"c"}', '20250102'),
        ('view', '2025-01-02 00:01:00', '{"p":"d"}', '20250102')
    """)
    print("  Created test_nopk_part")

    # Now introduce some differences for testing
    # Modify test_pk_nopart: change value for id=3 (will be detected as mismatch)
    dbx_sql("UPDATE test_pk_nopart SET value = 999 WHERE id = 3")
    # Add extra row in Databricks (only_in_dbx)
    dbx_sql("INSERT INTO test_pk_nopart VALUES (6, 'frank', 600)")
    print("  Introduced diffs in test_pk_nopart (id=3 value changed, id=6 added)")

    # Modify test_pk_part: change amount for id=1 in pt=20250101
    dbx_sql("UPDATE test_pk_part SET amount = 99 WHERE id = 1 AND pt = '20250101'")
    print("  Introduced diff in test_pk_part (id=1, pt=20250101 amount changed)")

    # Add extra row to test_nopk_nopart (count will differ)
    dbx_sql("INSERT INTO test_nopk_nopart VALUES ('scroll', '2025-01-01 00:03:00', '{\"page\":\"faq\"}')")
    print("  Introduced diff in test_nopk_nopart (extra row added)")

    # Add extra partition to test_nopk_part
    dbx_sql("INSERT INTO test_nopk_part VALUES ('click', '2025-01-03 00:00:00', '{\"p\":\"e\"}', '20250103')")
    print("  Introduced diff in test_nopk_part (extra partition 20250103)")

    print("Databricks setup complete!")


def setup_emr_tables(spark: SparkSession):
    """Create and populate test tables in EMR via Spark SQL (Iceberg catalog)."""
    print("Setting up EMR tables...")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {EMR_CATALOG}.{EMR_DB}")

    # 1. test_pk_nopart
    full_name = f"{EMR_CATALOG}.{EMR_DB}.test_pk_nopart"
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    spark.sql(f"""
        CREATE TABLE {full_name} (
            id INT, name STRING, value INT
        ) USING iceberg
    """)
    spark.sql(f"""
        INSERT INTO {full_name} VALUES
        (1, 'alice', 100),
        (2, 'bob', 200),
        (3, 'charlie', 300),
        (4, 'david', 400),
        (5, 'eve', 500)
    """)
    print(f"  Created {full_name}")

    # 2. test_pk_part
    full_name = f"{EMR_CATALOG}.{EMR_DB}.test_pk_part"
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    spark.sql(f"""
        CREATE TABLE {full_name} (
            id INT, name STRING, amount INT, pt STRING
        ) USING iceberg PARTITIONED BY (pt)
    """)
    spark.sql(f"""
        INSERT INTO {full_name} VALUES
        (1, 'alice', 10, '20250101'),
        (2, 'bob', 20, '20250101'),
        (3, 'charlie', 30, '20250102'),
        (4, 'david', 40, '20250102'),
        (5, 'eve', 50, '20250103')
    """)
    print(f"  Created {full_name}")

    # 3. test_nopk_nopart
    full_name = f"{EMR_CATALOG}.{EMR_DB}.test_nopk_nopart"
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    spark.sql(f"""
        CREATE TABLE {full_name} (
            event STRING, ts STRING, payload STRING
        ) USING iceberg
    """)
    spark.sql(f"""
        INSERT INTO {full_name} VALUES
        ('click', '2025-01-01 00:00:00', '{{"page":"home"}}'),
        ('view', '2025-01-01 00:01:00', '{{"page":"about"}}'),
        ('click', '2025-01-01 00:02:00', '{{"page":"shop"}}')
    """)
    print(f"  Created {full_name}")

    # 4. test_nopk_part
    full_name = f"{EMR_CATALOG}.{EMR_DB}.test_nopk_part"
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    spark.sql(f"""
        CREATE TABLE {full_name} (
            event STRING, ts STRING, payload STRING, pt STRING
        ) USING iceberg PARTITIONED BY (pt)
    """)
    spark.sql(f"""
        INSERT INTO {full_name} VALUES
        ('click', '2025-01-01 00:00:00', '{{"p":"a"}}', '20250101'),
        ('view', '2025-01-01 00:01:00', '{{"p":"b"}}', '20250101'),
        ('click', '2025-01-02 00:00:00', '{{"p":"c"}}', '20250102'),
        ('view', '2025-01-02 00:01:00', '{{"p":"d"}}', '20250102')
    """)
    print(f"  Created {full_name}")

    print("EMR setup complete! (EMR has the 'original' data, Databricks has modifications)")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode in ("dbx", "all"):
        setup_databricks_tables()

    if mode in ("emr", "all"):
        spark = SparkSession.builder \
            .appName("SetupTestData") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", EMR_WAREHOUSE) \
            .getOrCreate()
        setup_emr_tables(spark)
        spark.stop()

    print("\nDone! Expected diffs when comparing:")
    print("  test_pk_nopart: id=3 value mismatch (300 vs 999), id=6 only in DBX")
    print("  test_pk_part: id=1 pt=20250101 amount mismatch (10 vs 99)")
    print("  test_nopk_nopart: count 3(EMR) vs 4(DBX)")
    print("  test_nopk_part: partition 20250103 only in DBX")


if __name__ == "__main__":
    main()
