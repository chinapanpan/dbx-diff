"""
Create test tables for count-only (no numeric columns) verification.

Tables created:
1. test_no_numeric_multi_pt - partitioned, multiple pt values, pt=20250102 has diff
2. test_no_numeric_pt - partitioned, pt=20250102 has diff
3. test_count_only_nopart - non-partitioned, same data both sides

This script creates Databricks-side data.
"""
import os
import requests
import json

WORKSPACE_URL = os.environ.get("DATABRICKS_HOST", "https://dbc-51ad87e6-c26d.cloud.databricks.com")
TOKEN = os.environ["DATABRICKS_TOKEN"]
CATALOG = "workspace"
SCHEMA = "demo2"

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def run_sql(sql):
    url = f"{WORKSPACE_URL}/api/2.0/sql/statements"
    payload = {
        "statement": sql,
        "warehouse_id": "39e4573828a1dc00",
        "wait_timeout": "60s",
    }
    resp = requests.post(url, headers=HEADERS, json=payload)
    result = resp.json()
    if result.get("status", {}).get("state") == "FAILED":
        print(f"FAILED: {result['status'].get('error', {}).get('message', '')}")
        print(f"  SQL: {sql[:100]}")
    return result


def setup_tables():
    # Table 3: test_count_only_nopart (non-partitioned, no numeric cols)
    full = f"{CATALOG}.{SCHEMA}.test_count_only_nopart"
    run_sql(f"DROP TABLE IF EXISTS {full}")
    run_sql(f"""
        CREATE TABLE {full} (
            name STRING, city STRING, status STRING
        )
    """)
    run_sql(f"""
        INSERT INTO {full} VALUES
        ('alice','beijing','active'),
        ('bob','shanghai','inactive'),
        ('charlie','guangzhou','active'),
        ('david','shenzhen','active'),
        ('eve','hangzhou','inactive')
    """)
    print(f"Created {full}: 5 rows, non-partitioned")

    # Verify existing tables
    for tbl in ["test_no_numeric_multi_pt", "test_no_numeric_pt"]:
        full = f"{CATALOG}.{SCHEMA}.{tbl}"
        result = run_sql(f"SELECT count(*) as cnt FROM {full}")
        status = result.get("status", {}).get("state")
        if status == "SUCCEEDED":
            data = result.get("result", {}).get("data_array", [])
            print(f"Verified {full}: {data[0][0]} rows total")
        else:
            print(f"Warning: {full} status={status}")


if __name__ == "__main__":
    setup_tables()
