# dbx-diff: Databricks vs EMR Data Comparison Tool

A PySpark-based tool that validates data consistency between Databricks (Delta on S3) and EMR (Iceberg via Glue Catalog). Designed for production scale (5000+ tables) with high concurrency.

## Architecture

```
┌─────────────────┐         ┌──────────────────┐
│   Databricks    │         │   EMR Spark      │
│  (Delta on S3)  │         │  (Iceberg/Glue)  │
└────────┬────────┘         └────────┬─────────┘
         │                           │
         │  Unity Catalog REST API   │  SparkSession SQL
         │  → get S3 location        │  → direct query
         │  → spark.read.format      │
         │    ("delta").load(path)    │
         └───────────┬───────────────┘
                     │
              ┌──────┴──────┐
              │  dbx_diff.py │
              │  (PySpark    │
              │   Driver)    │
              └──────┬───────┘
                     │
              ThreadPoolExecutor
              (15-50 workers)
                     │
              ┌──────┴──────┐
              │  Markdown   │
              │  Report     │
              │  → S3       │
              └─────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `dbx_diff.py` | Main comparison engine (PySpark job) |
| `submit_job.py` | EMR Serverless / YARN submission script |
| `setup_test_data.py` | Test data setup for both engines |
| `tables.csv` | Example input CSV |

## Quick Start

### Prerequisites

- EMR 7.12 cluster or EMR Serverless 7.12 application
- Databricks workspace with Unity Catalog
- S3 bucket for code and reports
- Lake Formation permissions for EMR role to access Glue catalog

### Run on YARN (EMR cluster)

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"

python3 submit_job.py --mode yarn \
  --csv tables.csv \
  --s3-output s3://your-bucket/reports/diff_report.md \
  --databricks-host $DATABRICKS_HOST \
  --databricks-token $DATABRICKS_TOKEN
```

### Run on EMR Serverless

```bash
python3 submit_job.py --mode serverless \
  --application-id <emr-serverless-app-id> \
  --execution-role-arn arn:aws:iam::<account>:role/EMRServerlessExecutionRole \
  --csv s3://your-bucket/code/tables.csv \
  --s3-output s3://your-bucket/reports/diff_report.md \
  --databricks-host $DATABRICKS_HOST \
  --databricks-token $DATABRICKS_TOKEN
```

### Run directly with spark-submit

```bash
spark-submit \
  --master yarn \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  dbx_diff.py \
  --csv tables.csv \
  --s3-output s3://your-bucket/reports/diff_report.md \
  --databricks-host $DATABRICKS_HOST \
  --databricks-token $DATABRICKS_TOKEN
```

## Input CSV Format

Space-delimited CSV with the following columns (space is the delimiter because `primary_keys` itself uses commas internally):

| Column | Required | Description |
|--------|----------|-------------|
| `table_name` | Yes | Full path: `catalog.schema.table` (same name in both engines) |
| `primary_keys` | No | Comma-separated key columns (e.g. `id,name`) |
| `pt_start` | No | Partition range start (e.g. `20250101`) |
| `pt_end` | No | Partition range end (e.g. `20250610`) |
| `pt_keys` | No | Specific partitions to compare (e.g. `20250101,20250102`) |
| `dbx_location` | No | Pre-resolved S3 path for Databricks table (auto-resolved if empty) |

Example:
```
table_name primary_keys pt_start pt_end pt_keys
workspace.demo2.test_pk_nopart id
workspace.demo2.test_pk_part id,name 20250101 20250103 20250101,20250102
workspace.demo2.test_nopk_nopart
workspace.demo2.test_nopk_part  20250101 20250103 20250101,20250102
```

## Comparison Rules

| Scenario | Logic |
|----------|-------|
| **Primary keys + non-partitioned** | Full row-level diff: only-in-dbx, only-in-emr, value mismatches |
| **Primary keys + partitioned** | Compare partition count in range → per-partition record count → row-level diff for specified pt_keys |
| **No primary keys + non-partitioned** | Compare total record count |
| **No primary keys + partitioned** | Compare partition count in range → per-partition record count for specified pt_keys |

## Output

Markdown report uploaded to S3. Example sections:

```markdown
### workspace.demo2.test_pk_nopart — Row Check: **FAIL**

| Metric | Count |
|--------|-------|
| Only in Databricks | 1 |
| Only in EMR | 0 |
| Value Mismatched | 1 |

**Mismatched value samples** (up to 10):

| Keys | Column | Databricks | EMR |
|------|--------|------------|-----|
| {'id': '3'} | value | 999 | 300 |
```

## CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--csv` | (required) | Input CSV path (local or S3) |
| `--s3-output` | (required) | S3 path for output report |
| `--databricks-host` | env `DATABRICKS_HOST` | Databricks workspace URL |
| `--databricks-token` | env `DATABRICKS_TOKEN` | Databricks access token |
| `--workers` | 15 | Parallel workers for table comparison |
| `--timeout` | 600 | Timeout per table in seconds |
| `--emr-catalog` | `iceberg_catalog` | EMR Iceberg catalog name |
| `--emr-warehouse` | `s3://zpf-databricks-event/emr/demo2` | Iceberg warehouse path |
| `--mode` | `serverless` | Execution mode: `serverless` or `yarn` |

## EMR Serverless Setup

Requirements for EMR Serverless:
1. Application release: **emr-7.12.0**
2. VPC with NAT Gateway (for Databricks API access)
3. S3 Gateway endpoint + Glue Interface endpoint in VPC
4. Lake Formation permissions for the execution role
5. `pylibs.zip` containing `requests` library uploaded to S3

## Test Data

Use `setup_test_data.py` to create test tables in both engines:

```bash
# Set up both Databricks and EMR test data
export DATABRICKS_TOKEN="your-token"
spark-submit setup_test_data.py all
```

This creates 4 tables covering all comparison scenarios with intentional diffs.
