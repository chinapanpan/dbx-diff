"""
Create 90 test tables in Databricks and EMR for large-scale testing.

Table categories:
- 30 non-partitioned tables (test_nopart_001 ~ test_nopart_030)
- 30 tables partitioned by pt (test_pt_001 ~ test_pt_030)
- 30 tables partitioned by non-pt columns like dt, date_key (test_other_001 ~ test_other_030)

For each category:
- ~60% tables will have differences (FAIL)
- ~40% tables will match exactly (PASS)
"""

import sys
import os
import time
import requests
import random

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-51ad87e6-c26d.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DBX_CATALOG = "workspace"
DBX_SCHEMA = "demo2"


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
        raise Exception("No SQL warehouse found")
    payload["warehouse_id"] = warehouses[0]["id"]

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    if resp.status_code != 200:
        raise Exception(f"SQL failed: {resp.status_code} {resp.text}")
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


def create_nopart_tables_dbx(start, end):
    """Create non-partitioned tables in Databricks."""
    for i in range(start, end + 1):
        tbl = f"test_nopart_{i:03d}"
        print(f"  DBX creating {tbl}...")
        dbx_sql(f"DROP TABLE IF EXISTS {tbl}")
        dbx_sql(f"""
            CREATE TABLE {tbl} (
                id INT, name STRING, amount DOUBLE, score INT, category STRING
            ) USING DELTA
        """)
        # Base data
        rows = []
        for r in range(1, 6):
            rows.append(f"({r}, 'user_{r}', {r * 10.5}, {50 + r * 10}, 'cat_{r % 3}')")
        dbx_sql(f"INSERT INTO {tbl} VALUES {', '.join(rows)}")

        # ~60% tables get modifications to create differences
        if i % 5 != 0:  # 4 out of 5 = 80% have diffs for variety
            if i % 3 == 0:
                dbx_sql(f"UPDATE {tbl} SET amount = amount * 2 WHERE id = 1")
            elif i % 3 == 1:
                dbx_sql(f"INSERT INTO {tbl} VALUES (6, 'extra', 99.9, 100, 'cat_x')")
            else:
                dbx_sql(f"UPDATE {tbl} SET score = score + 50 WHERE id <= 2")


def create_pt_tables_dbx(start, end):
    """Create pt-partitioned tables in Databricks."""
    for i in range(start, end + 1):
        tbl = f"test_pt_{i:03d}"
        print(f"  DBX creating {tbl}...")
        dbx_sql(f"DROP TABLE IF EXISTS {tbl}")
        dbx_sql(f"""
            CREATE TABLE {tbl} (
                id INT, name STRING, amount DOUBLE, score INT, pt STRING
            ) USING DELTA PARTITIONED BY (pt)
        """)
        rows = []
        for pt in ['20250101', '20250102']:
            for r in range(1, 4):
                rows.append(f"({r + (0 if pt == '20250101' else 3)}, 'user_{r}', {r * 100.0}, {60 + r * 10}, '{pt}')")
        dbx_sql(f"INSERT INTO {tbl} VALUES {', '.join(rows)}")

        if i % 5 != 0:
            if i % 3 == 0:
                dbx_sql(f"UPDATE {tbl} SET amount = 999.0 WHERE id = 1 AND pt = '20250101'")
            elif i % 3 == 1:
                dbx_sql(f"INSERT INTO {tbl} VALUES (7, 'extra', 500.0, 95, '20250102')")
            else:
                dbx_sql(f"UPDATE {tbl} SET score = score + 100 WHERE pt = '20250101'")


def create_other_part_tables_dbx(start, end):
    """Create tables partitioned by non-pt columns (dt, date_key, region)."""
    part_cols = ['dt', 'date_key', 'region']
    for i in range(start, end + 1):
        tbl = f"test_other_{i:03d}"
        part_col = part_cols[i % 3]
        print(f"  DBX creating {tbl} (partitioned by {part_col})...")
        dbx_sql(f"DROP TABLE IF EXISTS {tbl}")

        if part_col == 'region':
            dbx_sql(f"""
                CREATE TABLE {tbl} (
                    id INT, name STRING, amount DOUBLE, score INT, region STRING
                ) USING DELTA PARTITIONED BY (region)
            """)
            rows = []
            for region in ['us', 'eu']:
                for r in range(1, 4):
                    rows.append(f"({r + (0 if region == 'us' else 3)}, 'user_{r}', {r * 50.0}, {40 + r * 15}, '{region}')")
            dbx_sql(f"INSERT INTO {tbl} VALUES {', '.join(rows)}")
        else:
            dbx_sql(f"""
                CREATE TABLE {tbl} (
                    id INT, name STRING, amount DOUBLE, score INT, {part_col} STRING
                ) USING DELTA PARTITIONED BY ({part_col})
            """)
            rows = []
            for d in ['20250101', '20250102']:
                for r in range(1, 4):
                    rows.append(f"({r + (0 if d == '20250101' else 3)}, 'user_{r}', {r * 75.0}, {55 + r * 12}, '{d}')")
            dbx_sql(f"INSERT INTO {tbl} VALUES {', '.join(rows)}")

        if i % 5 != 0:
            if i % 2 == 0:
                dbx_sql(f"UPDATE {tbl} SET amount = amount + 999.0 WHERE id = 1")
            else:
                dbx_sql(f"INSERT INTO {tbl} VALUES (7, 'extra', 888.0, 99, '{'ap' if part_col == 'region' else '20250103'}')")


def generate_emr_setup_script():
    """Generate the EMR-side setup script for Iceberg tables."""
    script = '''"""Auto-generated script to create 90 Iceberg test tables in EMR."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Setup90Tables").getOrCreate()

CATALOG = "workspace"
SCHEMA = "demo2"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")

def run(sql):
    spark.sql(sql)

'''
    # Non-partitioned tables
    for i in range(1, 31):
        tbl = f"test_nopart_{i:03d}"
        full = f"{{CATALOG}}.{{SCHEMA}}.{tbl}"
        script += f'print("Creating {tbl}...")\n'
        script += f'run(f"DROP TABLE IF EXISTS {full}")\n'
        script += f'run(f"""CREATE TABLE {full} (id INT, name STRING, amount DOUBLE, score INT, category STRING) USING iceberg""")\n'
        rows = []
        for r in range(1, 6):
            rows.append(f"({r}, 'user_{r}', {r * 10.5}, {50 + r * 10}, 'cat_{r % 3}')")
        script += f'run(f"""INSERT INTO {full} VALUES {", ".join(rows)}""")\n\n'

    # pt-partitioned tables
    for i in range(1, 31):
        tbl = f"test_pt_{i:03d}"
        full = f"{{CATALOG}}.{{SCHEMA}}.{tbl}"
        script += f'print("Creating {tbl}...")\n'
        script += f'run(f"DROP TABLE IF EXISTS {full}")\n'
        script += f'run(f"""CREATE TABLE {full} (id INT, name STRING, amount DOUBLE, score INT, pt STRING) USING iceberg PARTITIONED BY (pt)""")\n'
        rows = []
        for pt in ['20250101', '20250102']:
            for r in range(1, 4):
                rows.append(f"({r + (0 if pt == '20250101' else 3)}, 'user_{r}', {r * 100.0}, {60 + r * 10}, '{pt}')")
        script += f'run(f"""INSERT INTO {full} VALUES {", ".join(rows)}""")\n\n'

    # Other-partitioned tables
    part_cols = ['dt', 'date_key', 'region']
    for i in range(1, 31):
        tbl = f"test_other_{i:03d}"
        full = f"{{CATALOG}}.{{SCHEMA}}.{tbl}"
        part_col = part_cols[i % 3]
        script += f'print("Creating {tbl} (part by {part_col})...")\n'
        script += f'run(f"DROP TABLE IF EXISTS {full}")\n'

        if part_col == 'region':
            script += f'run(f"""CREATE TABLE {full} (id INT, name STRING, amount DOUBLE, score INT, region STRING) USING iceberg PARTITIONED BY (region)""")\n'
            rows = []
            for region in ['us', 'eu']:
                for r in range(1, 4):
                    rows.append(f"({r + (0 if region == 'us' else 3)}, 'user_{r}', {r * 50.0}, {40 + r * 15}, '{region}')")
        else:
            script += f'run(f"""CREATE TABLE {full} (id INT, name STRING, amount DOUBLE, score INT, {part_col} STRING) USING iceberg PARTITIONED BY ({part_col})""")\n'
            rows = []
            for d in ['20250101', '20250102']:
                for r in range(1, 4):
                    rows.append(f"({r + (0 if d == '20250101' else 3)}, 'user_{r}', {r * 75.0}, {55 + r * 12}, '{d}')")
        script += f'run(f"""INSERT INTO {full} VALUES {", ".join(rows)}""")\n\n'

    script += 'print("All 90 EMR tables created!")\nspark.stop()\n'
    return script


def generate_csv():
    """Generate tables.csv with all 93 tables (3 original + 90 new)."""
    lines = ["table_name"]
    lines.append("workspace.demo2.test_nopk_nopart")
    lines.append("workspace.demo2.test_nopk_part")
    lines.append("workspace.demo2.test_all_match")
    for i in range(1, 31):
        lines.append(f"workspace.demo2.test_nopart_{i:03d}")
    for i in range(1, 31):
        lines.append(f"workspace.demo2.test_pt_{i:03d}")
    for i in range(1, 31):
        lines.append(f"workspace.demo2.test_other_{i:03d}")
    return "\n".join(lines) + "\n"


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode in ("dbx", "all"):
        print("=== Creating Databricks tables ===")
        print("\n--- Non-partitioned tables (30) ---")
        create_nopart_tables_dbx(1, 30)
        print("\n--- pt-partitioned tables (30) ---")
        create_pt_tables_dbx(1, 30)
        print("\n--- Other-partitioned tables (30) ---")
        create_other_part_tables_dbx(1, 30)
        print("\nDatabricks: 90 tables created!")

    if mode in ("emr_script", "all"):
        script_content = generate_emr_setup_script()
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "setup_90_emr.py")
        with open(script_path, 'w') as f:
            f.write(script_content)
        print(f"\nGenerated EMR setup script: {script_path}")

    if mode in ("csv", "all"):
        csv_content = generate_csv()
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tables_90.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        print(f"\nGenerated CSV: {csv_path}")

    print("\nDone!")


if __name__ == "__main__":
    main()
