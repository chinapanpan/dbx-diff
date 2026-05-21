"""
基于 emr_common.Session 提交 dbx_diff.py 作业到 EMR Serverless 或 EMR on EC2。

Usage:
  # EMR Serverless
  python3 submit_job.py --mode serverless \
    --csv tables.csv \
    --s3-output s3://zpf-databricks-event/reports/diff_report.md \
    --databricks-host https://dbc-51ad87e6-c26d.cloud.databricks.com \
    --databricks-token <token>

  # EMR on EC2
  python3 submit_job.py --mode ec2 \
    --csv tables.csv \
    --s3-output s3://zpf-databricks-event/reports/diff_report.md \
    --databricks-host https://dbc-51ad87e6-c26d.cloud.databricks.com \
    --databricks-token <token>
"""

import argparse
import sys
import os

from emr_common import Session


EMR_SERVERLESS_APP_ID = "00g5qqg0spv6bq0l"
EXECUTION_ROLE_ARN = "arn:aws:iam::415797100173:role/EMRServerlessExecutionRole"
S3_LOGS_PATH = "s3://zpf-databricks-event/logs/"
S3_SCRIPTS_PATH = "s3://zpf-databricks-event/code/"
S3_VENV_ARCHIVE = "s3://zpf-databricks-event/code/pyspark_py312_venv.tar.gz"
REGION = "us-west-2"


def build_spark_conf(args) -> str:
    """构建 spark-submit 参数字符串。"""
    confs = [
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        f"--conf spark.sql.catalog.{args.emr_catalog}=org.apache.iceberg.spark.SparkCatalog",
        f"--conf spark.sql.catalog.{args.emr_catalog}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
        f"--conf spark.sql.catalog.{args.emr_catalog}.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        f"--conf spark.sql.catalog.{args.emr_catalog}.warehouse={args.emr_warehouse}",
        "--conf spark.executor.memory=4g",
        "--conf spark.executor.cores=4",
        "--conf spark.dynamicAllocation.enabled=true",
        "--conf spark.dynamicAllocation.maxExecutors=50",
        f"--conf spark.archives={args.venv_archive}#environment",
        "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python3.12",
        "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python3.12",
        "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python3.12",
        "--conf spark.emr-serverless.driverEnv.LD_LIBRARY_PATH=./environment/lib",
        "--conf spark.executorEnv.LD_LIBRARY_PATH=./environment/lib",
        "--conf spark.emr-serverless.driverEnv.PYTHONHOME=./environment",
        "--conf spark.executorEnv.PYTHONHOME=./environment",
        f"--conf spark.emr-serverless.driverEnv.DATABRICKS_HOST={args.databricks_host}",
    ]

    if args.databricks_secret_arn:
        pass
    elif args.databricks_token:
        confs.extend([
            f"--conf spark.emr-serverless.driverEnv.DATABRICKS_TOKEN={args.databricks_token}",
            f"--conf spark.executorEnv.DATABRICKS_TOKEN={args.databricks_token}",
        ])

    return " ".join(confs)


def build_job_args(args) -> list:
    """构建 dbx_diff.py 的入口参数。"""
    job_args = [
        "--csv", args.csv,
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


def main():
    parser = argparse.ArgumentParser(description="提交 dbx_diff 数据对比作业")
    parser.add_argument("--mode", choices=["serverless", "ec2"], default="serverless",
                        help="执行模式: serverless(EMR Serverless) 或 ec2(EMR on EC2)")
    parser.add_argument("--csv", required=True, help="输入 CSV 路径（本地或 S3）")
    parser.add_argument("--s3-output", required=True, help="S3 报告输出路径")
    parser.add_argument("--databricks-host", required=True, help="Databricks workspace URL")
    parser.add_argument("--databricks-token", default=None, help="Databricks 访问令牌")
    parser.add_argument("--databricks-secret-arn", default=None,
                        help="AWS Secrets Manager ARN（OAuth2 模式，优先于 token）")
    parser.add_argument("--workers", type=int, default=15, help="并行 worker 数（默认 15）")
    parser.add_argument("--timeout", type=int, default=600, help="单表超时秒数（默认 600）")
    parser.add_argument("--emr-catalog", default="iceberg_catalog", help="EMR Iceberg catalog 名称")
    parser.add_argument("--emr-warehouse", default="s3://zpf-databricks-event/emr/demo2",
                        help="Iceberg warehouse S3 路径")
    parser.add_argument("--application-id", default=EMR_SERVERLESS_APP_ID,
                        help="EMR Serverless 应用 ID")
    parser.add_argument("--execution-role-arn", default=EXECUTION_ROLE_ARN,
                        help="EMR Serverless 执行角色 ARN")
    parser.add_argument("--venv-archive", default=S3_VENV_ARCHIVE,
                        help="Python 3.12 venv tar.gz 的 S3 路径")
    args = parser.parse_args()

    jobtype = 1 if args.mode == "serverless" else 0

    session = Session(
        application_id=args.application_id,
        jobtype=jobtype,
        region=REGION,
        job_role=args.execution_role_arn,
        logs_s3_path=S3_LOGS_PATH,
        script_s3_path=S3_SCRIPTS_PATH,
        spark_conf=build_spark_conf(args),
    )

    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbx_diff.py")
    job_args = build_job_args(args)

    print(f"提交模式: {args.mode}")
    print(f"应用 ID: {args.application_id}")
    print(f"输入 CSV: {args.csv}")
    print(f"输出报告: {args.s3_output}")

    result = session.submit_file("dbx_diff", script_path, args=job_args)

    if result.status in ("SUCCESS", "COMPLETED"):
        print(f"\n作业成功完成。报告路径: {args.s3_output}")
        sys.exit(0)
    else:
        print(f"\n作业失败，状态: {result.status}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
