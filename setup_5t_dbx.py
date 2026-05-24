"""
Create 5 new Delta test tables in workspace.demo2 for widget-rename verification.
Tables: v5_t2_part_match, v5_t2_part_mismatch, v5_t2_nopart_str,
        v5_t2_nopart_num_diff, v5_t2_part_count_only
Partitions: pt=20260524/25/26
Storage: s3://zpf-databricks-event/delta_tables/
"""

import sys
import time
import requests
import boto3


def get_token(host, secret_arn, region="us-west-2"):
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_arn)
    secret_str = resp["SecretString"].strip()
    if ":" in secret_str and not secret_str.startswith("dapi"):
        cid, csec = secret_str.split(":", 1)
        r = requests.post(f"{host}/oidc/v1/token",
                          data={"grant_type": "client_credentials", "scope": "all-apis"},
                          auth=(cid, csec), timeout=30)
        return r.json()["access_token"]
    return secret_str


def execute_sql(host, token, sql, warehouse_id):
    url = f"{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"statement": sql, "warehouse_id": warehouse_id, "wait_timeout": "50s", "disposition": "INLINE"}
    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    if resp.status_code != 200:
        raise Exception(f"SQL failed: {resp.status_code} {resp.text[:200]}")
    result = resp.json()
    status = result.get("status", {}).get("state", "")
    if status == "FAILED":
        raise Exception(f"SQL error: {result.get('status', {}).get('error', {})}")
    if status in ("PENDING", "RUNNING"):
        sid = result.get("statement_id")
        for _ in range(30):
            time.sleep(2)
            cr = requests.get(f"{url}/{sid}", headers=headers, timeout=30).json()
            cs = cr.get("status", {}).get("state", "")
            if cs == "SUCCEEDED":
                return cr
            if cs == "FAILED":
                raise Exception(f"SQL error: {cr.get('status', {}).get('error', {})}")
        raise Exception("SQL timeout")
    return result


def get_warehouse_id(host, token):
    url = f"{host}/api/2.0/sql/warehouses"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    for wh in resp.json().get("warehouses", []):
        if wh.get("state") in ("RUNNING", "STARTING"):
            return wh["id"]
    for wh in resp.json().get("warehouses", []):
        return wh["id"]
    raise Exception("No warehouse found")


S3_BASE = "s3://zpf-databricks-event/delta_tables"


def main():
    host = "https://dbc-51ad87e6-c26d.cloud.databricks.com"
    secret_arn = "arn:aws:secretsmanager:us-west-2:785682719467:secret:databricks-6lajvp"

    print("Getting credentials...")
    token = get_token(host, secret_arn)
    wh = get_warehouse_id(host, token)
    print(f"Warehouse: {wh}")

    sqls = [
        # --- 1. v5_t2_part_match: partitioned + numeric, fully matching ---
        "DROP TABLE IF EXISTS workspace.demo2.v5_t2_part_match",
        f"""CREATE TABLE workspace.demo2.v5_t2_part_match (
            id BIGINT, revenue DOUBLE, qty INT, pt STRING
        ) USING DELTA PARTITIONED BY (pt) LOCATION '{S3_BASE}/v5_t2_part_match'""",
        """INSERT INTO workspace.demo2.v5_t2_part_match VALUES
        (1, 150.0, 5, '20260524'), (2, 250.0, 8, '20260524'), (3, 350.0, 12, '20260524'),
        (4, 450.0, 15, '20260525'), (5, 550.0, 20, '20260525'),
        (6, 650.0, 25, '20260526'), (7, 750.0, 30, '20260526'), (8, 850.0, 35, '20260526')""",

        # --- 2. v5_t2_part_mismatch: partitioned + numeric, pt=25 differs ---
        "DROP TABLE IF EXISTS workspace.demo2.v5_t2_part_mismatch",
        f"""CREATE TABLE workspace.demo2.v5_t2_part_mismatch (
            id BIGINT, cost DOUBLE, units INT, pt STRING
        ) USING DELTA PARTITIONED BY (pt) LOCATION '{S3_BASE}/v5_t2_part_mismatch'""",
        """INSERT INTO workspace.demo2.v5_t2_part_mismatch VALUES
        (1, 11.0, 2, '20260524'), (2, 22.0, 4, '20260524'),
        (3, 33.0, 6, '20260525'), (4, 888.0, 77, '20260525'),
        (5, 55.0, 10, '20260526'), (6, 66.0, 12, '20260526')""",

        # --- 3. v5_t2_nopart_str: non-partitioned, no numeric columns, count match ---
        "DROP TABLE IF EXISTS workspace.demo2.v5_t2_nopart_str",
        f"""CREATE TABLE workspace.demo2.v5_t2_nopart_str (
            uid STRING, city STRING, dept STRING
        ) USING DELTA LOCATION '{S3_BASE}/v5_t2_nopart_str'""",
        """INSERT INTO workspace.demo2.v5_t2_nopart_str VALUES
        ('u1', 'Beijing', 'Eng'), ('u2', 'Shanghai', 'Sales'),
        ('u3', 'Shenzhen', 'Eng'), ('u4', 'Hangzhou', 'PM'),
        ('u5', 'Guangzhou', 'Sales')""",

        # --- 4. v5_t2_nopart_num_diff: non-partitioned + numeric, sum differs ---
        "DROP TABLE IF EXISTS workspace.demo2.v5_t2_nopart_num_diff",
        f"""CREATE TABLE workspace.demo2.v5_t2_nopart_num_diff (
            id BIGINT, weight DOUBLE, height INT, label STRING
        ) USING DELTA LOCATION '{S3_BASE}/v5_t2_nopart_num_diff'""",
        """INSERT INTO workspace.demo2.v5_t2_nopart_num_diff VALUES
        (1, 60.5, 170, 'A'), (2, 75.0, 180, 'B'), (3, 55.2, 165, 'C'),
        (4, 90.0, 190, 'D'), (5, 999.9, 200, 'extra_in_dbx')""",

        # --- 5. v5_t2_part_count_only: partitioned, no numeric, pt=26 count differs ---
        "DROP TABLE IF EXISTS workspace.demo2.v5_t2_part_count_only",
        f"""CREATE TABLE workspace.demo2.v5_t2_part_count_only (
            code STRING, region STRING, flag STRING, pt STRING
        ) USING DELTA PARTITIONED BY (pt) LOCATION '{S3_BASE}/v5_t2_part_count_only'""",
        """INSERT INTO workspace.demo2.v5_t2_part_count_only VALUES
        ('c1', 'east', 'Y', '20260524'), ('c2', 'west', 'N', '20260524'),
        ('c3', 'east', 'Y', '20260525'), ('c4', 'north', 'Y', '20260525'), ('c5', 'south', 'N', '20260525'),
        ('c6', 'east', 'Y', '20260526'), ('c7', 'west', 'N', '20260526'), ('c8', 'north', 'Y', '20260526'), ('c9', 'extra', 'Y', '20260526')""",
    ]

    for i, sql in enumerate(sqls):
        sql = sql.strip()
        if not sql:
            continue
        print(f"[{i+1}/{len(sqls)}] {sql[:80]}...")
        try:
            execute_sql(host, token, sql, wh)
            print("  OK")
        except Exception as e:
            print(f"  ERROR: {e}")
            if "DROP" not in sql and "IF NOT EXISTS" not in sql:
                sys.exit(1)

    print("\n5 Databricks Delta tables created!")


if __name__ == "__main__":
    main()
