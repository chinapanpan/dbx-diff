"""
Submit dbx_diff.py as a Spark job on EMR Serverless or YARN.

Usage:
  # EMR Serverless
  python3 submit_job.py --mode serverless \
    --application-id 00g5qq35gelmn30l \
    --execution-role-arn arn:aws:iam::415797100173:role/EMRServerlessExecutionRole \
    --csv s3://zpf-databricks-event/config/tables.csv \
    --s3-output s3://zpf-databricks-event/reports/diff_report.md \
    --databricks-host https://dbc-51ad87e6-c26d.cloud.databricks.com \
    --databricks-token <token>

  # YARN (on EMR cluster)
  python3 submit_job.py --mode yarn \
    --csv tables.csv \
    --s3-output s3://zpf-databricks-event/reports/diff_report.md \
    --databricks-host https://dbc-51ad87e6-c26d.cloud.databricks.com \
    --databricks-token <token>
"""

import argparse
import subprocess
import sys
import os
import json
import time

import boto3


EMR_SERVERLESS_APP_ID = "00g5qqg0spv6bq0l"
EXECUTION_ROLE_ARN = "arn:aws:iam::415797100173:role/EMRServerlessExecutionRole"
S3_CODE_BUCKET = "s3://zpf-databricks-event/code"
S3_PYLIBS = "s3://zpf-databricks-event/code/pylibs.zip"


def upload_script_to_s3():
    """Upload dbx_diff.py to S3 so EMR Serverless can access it."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_path = os.path.join(script_dir, "dbx_diff.py")
    s3_path = f"{S3_CODE_BUCKET}/dbx_diff.py"

    s3 = boto3.client("s3")
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    s3.upload_file(local_path, bucket, key)
    print(f"Uploaded dbx_diff.py to {s3_path}")
    return s3_path


def resolve_and_upload_csv(args) -> str:
    """Resolve Databricks table locations and upload enriched CSV to S3.

    This runs on the submit host (with internet access to Databricks API)
    so that EMR Serverless doesn't need outbound internet.
    """
    import csv as csv_mod
    import io
    import requests

    # Read original CSV
    if args.csv.startswith("s3://"):
        s3 = boto3.client("s3")
        bucket, key = args.csv.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
    else:
        with open(args.csv, 'r') as f:
            content = f.read()

    reader = csv_mod.DictReader(io.StringIO(content), delimiter=' ')
    rows = list(reader)

    # Resolve locations via Databricks API
    headers = {"Authorization": f"Bearer {args.databricks_token}"}
    for row in rows:
        if row.get('dbx_location', '').strip():
            continue
        table_name = row['table_name'].strip()
        url = f"{args.databricks_host}/api/2.1/unity-catalog/tables/{table_name}"
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            row['dbx_location'] = resp.json().get('storage_location', '')
            print(f"  Resolved {table_name} -> {row['dbx_location']}")
        else:
            print(f"  WARNING: Could not resolve {table_name}: {resp.status_code}")
            row['dbx_location'] = ''

    # Write enriched CSV (space-delimited)
    output = io.StringIO()
    fieldnames = ['table_name', 'primary_keys', 'pt_start', 'pt_end', 'pt_keys', 'dbx_location']
    writer = csv_mod.DictWriter(output, fieldnames=fieldnames, delimiter=' ')
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k, '') for k in fieldnames})

    # Upload to S3
    s3_csv_path = f"{S3_CODE_BUCKET}/tables_resolved.csv"
    s3 = boto3.client("s3")
    bucket, key = s3_csv_path.replace("s3://", "").split("/", 1)
    s3.put_object(Bucket=bucket, Key=key, Body=output.getvalue().encode("utf-8"))
    print(f"Uploaded resolved CSV to {s3_csv_path}")
    return s3_csv_path


def _build_job_args(args, s3_csv_path: str) -> list:
    """Build entryPointArguments for the Spark job."""
    job_args = [
        "--csv", s3_csv_path,
        "--s3-output", args.s3_output,
        "--workers", str(args.workers),
        "--timeout", str(args.timeout),
        "--emr-catalog", args.emr_catalog,
        "--emr-warehouse", args.emr_warehouse,
        "--databricks-host", args.databricks_host,
    ]
    if args.databricks_secret_arn:
        job_args.extend(["--databricks-secret-arn", args.databricks_secret_arn])
    elif args.databricks_token:
        job_args.extend(["--databricks-token", args.databricks_token])
    return job_args


def submit_serverless(args):
    """Submit job to EMR Serverless.

    The app must be configured with VPC subnets that have NAT Gateway
    for outbound internet access to Databricks API.
    """
    s3_script = upload_script_to_s3()

    # Upload CSV to S3 if it's a local path
    if not args.csv.startswith("s3://"):
        s3 = boto3.client("s3")
        bucket, key = S3_CODE_BUCKET.replace("s3://", "").split("/", 1)
        csv_key = key + "/tables_input.csv"
        s3.upload_file(args.csv, bucket, csv_key)
        s3_csv_path = f"s3://{bucket}/{csv_key}"
        print(f"Uploaded CSV to {s3_csv_path}")
    else:
        s3_csv_path = args.csv

    spark_submit_params = [
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", f"spark.sql.catalog.{args.emr_catalog}=org.apache.iceberg.spark.SparkCatalog",
        "--conf", f"spark.sql.catalog.{args.emr_catalog}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
        "--conf", f"spark.sql.catalog.{args.emr_catalog}.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "--conf", f"spark.sql.catalog.{args.emr_catalog}.warehouse={args.emr_warehouse}",
        "--conf", "spark.executor.memory=4g",
        "--conf", "spark.executor.cores=4",
        "--conf", "spark.dynamicAllocation.enabled=true",
        "--conf", "spark.dynamicAllocation.maxExecutors=50",
        "--conf", f"spark.submit.pyFiles={S3_PYLIBS}",
        "--conf", "spark.emr-serverless.driverEnv.DATABRICKS_HOST=" + args.databricks_host,
    ]

    # Pass credentials via env vars or secret ARN
    if args.databricks_secret_arn:
        # OAuth2 mode: pass secret ARN as job argument, no token in env
        pass
    elif args.databricks_token:
        spark_submit_params.extend([
            "--conf", "spark.emr-serverless.driverEnv.DATABRICKS_TOKEN=" + args.databricks_token,
            "--conf", "spark.executorEnv.DATABRICKS_TOKEN=" + args.databricks_token,
        ])

    client = boto3.client("emr-serverless", region_name="us-west-2")

    response = client.start_job_run(
        applicationId=args.application_id,
        executionRoleArn=args.execution_role_arn,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": s3_script,
                "entryPointArguments": _build_job_args(args, s3_csv_path),
                "sparkSubmitParameters": " ".join(spark_submit_params),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "managedPersistenceMonitoringConfiguration": {
                    "enabled": True
                }
            }
        },
    )

    job_run_id = response["jobRunId"]
    print(f"Submitted EMR Serverless job: {job_run_id}")
    print(f"Application ID: {args.application_id}")

    # Poll for completion
    print("Waiting for job to complete...")
    terminal_states = {"SUCCESS", "FAILED", "CANCELLING", "CANCELLED"}
    while True:
        time.sleep(15)
        status_resp = client.get_job_run(
            applicationId=args.application_id,
            jobRunId=job_run_id,
        )
        state = status_resp["jobRun"]["state"]
        print(f"  State: {state}")
        if state in terminal_states:
            break

    if state == "SUCCESS":
        print(f"Job completed successfully. Report at: {args.s3_output}")
        return 0
    else:
        details = status_resp["jobRun"].get("stateDetails", "")
        print(f"Job failed with state: {state}. Details: {details}", file=sys.stderr)
        return 1


def submit_yarn(args):
    """Submit via spark-submit on YARN (EMR cluster mode)."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbx_diff_path = os.path.join(script_dir, "dbx_diff.py")

    cmd = [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", f"spark.sql.catalog.{args.emr_catalog}=org.apache.iceberg.spark.SparkCatalog",
        "--conf", f"spark.sql.catalog.{args.emr_catalog}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
        "--conf", f"spark.sql.catalog.{args.emr_catalog}.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "--conf", f"spark.sql.catalog.{args.emr_catalog}.warehouse={args.emr_warehouse}",
        "--conf", "spark.executor.memory=4g",
        "--conf", "spark.executor.cores=2",
        "--conf", "spark.dynamicAllocation.enabled=true",
        "--conf", "spark.dynamicAllocation.maxExecutors=20",
        dbx_diff_path,
        "--csv", args.csv,
        "--s3-output", args.s3_output,
        "--workers", str(args.workers),
        "--timeout", str(args.timeout),
        "--emr-catalog", args.emr_catalog,
        "--emr-warehouse", args.emr_warehouse,
        "--databricks-host", args.databricks_host,
    ]
    if args.databricks_secret_arn:
        cmd.extend(["--databricks-secret-arn", args.databricks_secret_arn])
    elif args.databricks_token:
        cmd.extend(["--databricks-token", args.databricks_token])

    env = os.environ.copy()
    env["DATABRICKS_HOST"] = args.databricks_host

    print(f"Submitting spark job on YARN...")
    result = subprocess.run(cmd, env=env, capture_output=False, text=True)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Submit DbxDiff Spark Job")
    parser.add_argument("--mode", choices=["serverless", "yarn"], default="serverless", help="Execution mode")
    parser.add_argument("--csv", required=True, help="Path to input CSV file (S3 path for serverless, local for yarn)")
    parser.add_argument("--s3-output", required=True, help="S3 output path for report")
    parser.add_argument("--databricks-host", required=True, help="Databricks workspace URL")
    parser.add_argument("--databricks-token", default=None, help="Databricks access token")
    parser.add_argument("--databricks-secret-arn", default=None,
                        help="AWS Secrets Manager ARN for OAuth2 credentials (overrides --databricks-token)")
    parser.add_argument("--workers", type=int, default=15, help="Parallel workers (default: 15)")
    parser.add_argument("--timeout", type=int, default=600, help="Timeout per table in seconds (default: 600)")
    parser.add_argument("--emr-catalog", default="iceberg_catalog", help="EMR Iceberg catalog name")
    parser.add_argument("--emr-warehouse", default="s3://zpf-databricks-event/emr/demo2", help="EMR Iceberg warehouse S3 path")
    parser.add_argument("--application-id", default=EMR_SERVERLESS_APP_ID, help="EMR Serverless application ID")
    parser.add_argument("--execution-role-arn", default=EXECUTION_ROLE_ARN, help="EMR Serverless execution role ARN")
    args = parser.parse_args()

    if args.mode == "serverless":
        rc = submit_serverless(args)
    else:
        rc = submit_yarn(args)

    sys.exit(rc)


if __name__ == "__main__":
    main()
