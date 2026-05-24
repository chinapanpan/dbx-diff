"""
Setup test data on Databricks side using SQL Statement API.

Creates tables in workspace.demo2 on Databricks with:
- v5_test_partitioned: same data for pt=20260521, DIFFERENT data for pt=20260522
- v5_test_nopart: same data as EMR side (exact match expected)
- v5_test_no_numeric: same count for pt=20260521, DIFFERENT count for pt=20260522

This allows testing:
1. Partitioned with numeric cols: pt=20260521 PASS, pt=20260522 FAIL
2. Non-partitioned with numeric cols: PASS
3. Partitioned count-only (no numeric): pt=20260521 PASS, pt=20260522 FAIL
"""

import sys
import os
import time
import json
import requests
import boto3


def get_databricks_credentials(host: str, secret_arn: str, region: str = "us-west-2"):
    """Get Databricks access token from Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_arn)
    secret_str = resp["SecretString"].strip()

    if ":" in secret_str and not secret_str.startswith("dapi"):
        client_id, client_secret = secret_str.split(":", 1)
        url = f"{host}/oidc/v1/token"
        data = {"grant_type": "client_credentials", "scope": "all-apis"}
        resp = requests.post(url, data=data, auth=(client_id, client_secret), timeout=30)
        if resp.status_code != 200:
            raise Exception(f"OAuth2 token request failed: {resp.status_code} {resp.text}")
        return resp.json()["access_token"]
    else:
        return secret_str


def execute_sql(host: str, token: str, sql: str, warehouse_id: str):
    """Execute SQL on Databricks using SQL Statement API."""
    url = f"{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "50s",
        "disposition": "INLINE",
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    if resp.status_code != 200:
        raise Exception(f"SQL execution failed: {resp.status_code} {resp.text}")
    result = resp.json()
    status = result.get("status", {}).get("state", "")
    if status == "FAILED":
        error = result.get("status", {}).get("error", {})
        raise Exception(f"SQL failed: {error}")
    if status == "PENDING" or status == "RUNNING":
        statement_id = result.get("statement_id")
        for _ in range(30):
            time.sleep(2)
            check_url = f"{host}/api/2.0/sql/statements/{statement_id}"
            check_resp = requests.get(check_url, headers=headers, timeout=30)
            check_result = check_resp.json()
            check_status = check_result.get("status", {}).get("state", "")
            if check_status == "SUCCEEDED":
                return check_result
            elif check_status == "FAILED":
                error = check_result.get("status", {}).get("error", {})
                raise Exception(f"SQL failed: {error}")
        raise Exception("SQL execution timeout")
    return result


def get_warehouse_id(host: str, token: str) -> str:
    """Get an available SQL warehouse ID."""
    url = f"{host}/api/2.0/sql/warehouses"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"Failed to list warehouses: {resp.status_code}")
    warehouses = resp.json().get("warehouses", [])
    for wh in warehouses:
        if wh.get("state") in ("RUNNING", "STARTING"):
            return wh["id"]
    for wh in warehouses:
        return wh["id"]
    raise Exception("No SQL warehouse found")


def main():
    host = "https://dbc-51ad87e6-c26d.cloud.databricks.com"
    secret_arn = "arn:aws:secretsmanager:us-west-2:785682719467:secret:databricks-6lajvp"

    print("Getting Databricks credentials...")
    token = get_databricks_credentials(host, secret_arn)
    print("Getting warehouse ID...")
    warehouse_id = get_warehouse_id(host, token)
    print(f"Using warehouse: {warehouse_id}")

    sqls = [
        # Create schema
        "CREATE SCHEMA IF NOT EXISTS workspace.demo2",

        # 1. Partitioned table with numeric columns
        "DROP TABLE IF EXISTS workspace.demo2.v5_test_partitioned",
        """
        CREATE TABLE workspace.demo2.v5_test_partitioned (
            id BIGINT,
            name STRING,
            amount DOUBLE,
            quantity INT,
            pt STRING
        )
        PARTITIONED BY (pt)
        """,
        # pt=20260521 matches EMR exactly
        """
        INSERT INTO workspace.demo2.v5_test_partitioned VALUES
        (1, 'alice', 100.0, 10, '20260521'),
        (2, 'bob', 200.0, 20, '20260521'),
        (3, 'carol', 300.0, 30, '20260521'),
        (4, 'dave', 150.0, 15, '20260522'),
        (5, 'eve', 250.0, 25, '20260522'),
        (6, 'frank', 999.0, 99, '20260522')
        """,
        # Note: pt=20260522 frank has amount=999.0,quantity=99 vs EMR's 350.0,35

        # 2. Non-partitioned table with numeric columns (exact match)
        "DROP TABLE IF EXISTS workspace.demo2.v5_test_nopart",
        """
        CREATE TABLE workspace.demo2.v5_test_nopart (
            id BIGINT,
            value DOUBLE,
            score INT,
            label STRING
        )
        """,
        """
        INSERT INTO workspace.demo2.v5_test_nopart VALUES
        (1, 10.5, 80, 'good'),
        (2, 20.3, 90, 'excellent'),
        (3, 30.7, 70, 'average'),
        (4, 40.1, 85, 'good'),
        (5, 50.9, 95, 'excellent')
        """,

        # 3. Partitioned table without numeric columns (count-only)
        "DROP TABLE IF EXISTS workspace.demo2.v5_test_no_numeric",
        """
        CREATE TABLE workspace.demo2.v5_test_no_numeric (
            id STRING,
            name STRING,
            status STRING,
            pt STRING
        )
        PARTITIONED BY (pt)
        """,
        # pt=20260521 has 3 rows (match), pt=20260522 has 3 rows (EMR has 2 → FAIL)
        """
        INSERT INTO workspace.demo2.v5_test_no_numeric VALUES
        ('a1', 'alice', 'active', '20260521'),
        ('a2', 'bob', 'active', '20260521'),
        ('a3', 'carol', 'inactive', '20260521'),
        ('a4', 'dave', 'active', '20260522'),
        ('a5', 'eve', 'inactive', '20260522'),
        ('a6', 'grace', 'active', '20260522')
        """,
    ]

    for i, sql in enumerate(sqls):
        sql = sql.strip()
        if not sql:
            continue
        print(f"\n[{i+1}/{len(sqls)}] Executing: {sql[:80]}...")
        try:
            execute_sql(host, token, sql, warehouse_id)
            print("  OK")
        except Exception as e:
            print(f"  ERROR: {e}")
            if "DROP" not in sql and "IF NOT EXISTS" not in sql:
                sys.exit(1)

    print("\n" + "=" * 60)
    print("Databricks test tables created successfully!")
    print("Expected test results:")
    print("  - v5_test_partitioned: pt=20260521 PASS, pt=20260522 FAIL (amount/quantity differ)")
    print("  - v5_test_nopart: PASS (exact match)")
    print("  - v5_test_no_numeric: pt=20260521 PASS, pt=20260522 FAIL (count differs: 3 vs 2)")
    print("=" * 60)


if __name__ == "__main__":
    main()
