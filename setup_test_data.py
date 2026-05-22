"""
Set up test data in Databricks (workspace.demo2) and EMR (Iceberg catalog)
for aggregate-based comparison testing (v3).

Test tables:
1. test_nopk_nopart — non-partitioned, numeric columns (aggregate diff)
2. test_nopk_part  — partitioned by pt, numeric columns (per-partition aggregate diff)
"""

import sys
import os
import time
import requests


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
        "wait_timeout": "50s" if wait else "0s",
        "catalog": DBX_CATALOG,
        "schema": DBX_SCHEMA,
    }
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

    # 1. test_nopk_nopart — non-partitioned with numeric columns
    dbx_sql("DROP TABLE IF EXISTS test_nopk_nopart")
    dbx_sql("""
        CREATE TABLE test_nopk_nopart (
            id INT, name STRING, value DOUBLE, score INT
        ) USING DELTA
    """)
    dbx_sql("""
        INSERT INTO test_nopk_nopart VALUES
        (1, 'alice', 10.5, 80),
        (2, 'bob', 20.3, 90),
        (3, 'charlie', 30.7, 85),
        (4, 'david', 40.1, 70),
        (5, 'eve', 50.9, 95)
    """)
    # Introduce difference: modify one row's value (will change max/avg)
    dbx_sql("UPDATE test_nopk_nopart SET value = 99.9 WHERE id = 5")
    # Add extra row (will change count)
    dbx_sql("INSERT INTO test_nopk_nopart VALUES (6, 'frank', 60.0, 88)")
    print("  Created test_nopk_nopart (6 rows, value[5] modified, extra row)")

    # 2. test_nopk_part — partitioned by pt with numeric columns
    dbx_sql("DROP TABLE IF EXISTS test_nopk_part")
    dbx_sql("""
        CREATE TABLE test_nopk_part (
            id INT, name STRING, amount DOUBLE, score INT, pt STRING
        ) USING DELTA PARTITIONED BY (pt)
    """)
    dbx_sql("""
        INSERT INTO test_nopk_part VALUES
        (1, 'alice', 100.0, 80, '20250101'),
        (2, 'bob', 200.0, 90, '20250101'),
        (3, 'charlie', 150.0, 85, '20250101'),
        (4, 'david', 300.0, 70, '20250102'),
        (5, 'eve', 250.0, 95, '20250102'),
        (6, 'frank', 180.0, 88, '20250102')
    """)
    # Introduce difference in pt=20250101: change amount for one row
    dbx_sql("UPDATE test_nopk_part SET amount = 999.0 WHERE id = 1 AND pt = '20250101'")
    # Add extra row in pt=20250102 (will change count for that partition)
    dbx_sql("INSERT INTO test_nopk_part VALUES (7, 'grace', 400.0, 92, '20250102')")
    print("  Created test_nopk_part (pt=20250101: amount[1] modified; pt=20250102: extra row)")

    print("Databricks setup complete!")


def setup_emr_tables(spark):
    """Create and populate test tables in EMR (Iceberg catalog) — baseline data."""
    print("Setting up EMR tables...")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {EMR_CATALOG}.{EMR_DB}")

    # 1. test_nopk_nopart — same baseline as Databricks before modifications
    full_name = f"{EMR_CATALOG}.{EMR_DB}.test_nopk_nopart"
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    spark.sql(f"""
        CREATE TABLE {full_name} (
            id INT, name STRING, value DOUBLE, score INT
        ) USING iceberg
    """)
    spark.sql(f"""
        INSERT INTO {full_name} VALUES
        (1, 'alice', 10.5, 80),
        (2, 'bob', 20.3, 90),
        (3, 'charlie', 30.7, 85),
        (4, 'david', 40.1, 70),
        (5, 'eve', 50.9, 95)
    """)
    print(f"  Created {full_name} (5 rows, original data)")

    # 2. test_nopk_part — same baseline
    full_name = f"{EMR_CATALOG}.{EMR_DB}.test_nopk_part"
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    spark.sql(f"""
        CREATE TABLE {full_name} (
            id INT, name STRING, amount DOUBLE, score INT, pt STRING
        ) USING iceberg PARTITIONED BY (pt)
    """)
    spark.sql(f"""
        INSERT INTO {full_name} VALUES
        (1, 'alice', 100.0, 80, '20250101'),
        (2, 'bob', 200.0, 90, '20250101'),
        (3, 'charlie', 150.0, 85, '20250101'),
        (4, 'david', 300.0, 70, '20250102'),
        (5, 'eve', 250.0, 95, '20250102'),
        (6, 'frank', 180.0, 88, '20250102')
    """)
    print(f"  Created {full_name} (6 rows, original data)")

    print("EMR setup complete! (EMR has baseline data, Databricks has modifications)")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode in ("dbx", "all"):
        setup_databricks_tables()

    if mode in ("emr", "all"):
        from pyspark.sql import SparkSession
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
    print("  test_nopk_nopart: count 5(ICE) vs 6(DELTA), value max 50.9(ICE) vs 99.9(DELTA)")
    print("  test_nopk_part pt=20250101: amount max 200(ICE) vs 999(DELTA)")
    print("  test_nopk_part pt=20250102: count 3(ICE) vs 4(DELTA)")


if __name__ == "__main__":
    main()
