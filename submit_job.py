"""
基于 emr_common.Session 提交 dbx_diff.py 作业到 EMR Serverless。

Spark 相关配置（Iceberg catalog、venv、动态分配等）已在 EMR Serverless Application 级别配置，
本脚本仅需传入业务参数。

Usage:
  python3 submit_job.py \
    --csv s3://zpf-databricks-event/code/tables.csv \
    --s3-output s3://zpf-databricks-event/reports/diff_report.md \
    --databricks-host https://dbc-51ad87e6-c26d.cloud.databricks.com \
    --databricks-secret-arn arn:aws:secretsmanager:us-west-2:<account>:secret:<name>
"""

import argparse
import sys
import os

from emr_common import Session


EMR_SERVERLESS_APP_ID = "00g5qqg0spv6bq0l"
EXECUTION_ROLE_ARN = "arn:aws:iam::415797100173:role/EMRServerlessExecutionRole"
S3_LOGS_PATH = "s3://zpf-databricks-event/logs/"
S3_SCRIPTS_PATH = "s3://zpf-databricks-event/code/"
REGION = "us-west-2"


def build_spark_conf(args) -> str:
    """构建运行时需要的环境变量配置（仅 Databricks 凭证相关）。"""
    confs = [
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
        "--databricks-host", args.databricks_host,
    ]
    if args.databricks_secret_arn:
        job_args.extend(["--databricks-secret-arn", args.databricks_secret_arn])
    elif args.databricks_token:
        job_args.extend(["--databricks-token", args.databricks_token])
    return job_args


def main():
    parser = argparse.ArgumentParser(description="提交 dbx_diff 数据对比作业")
    parser.add_argument("--csv", required=True, help="输入 CSV 路径（S3 路径）")
    parser.add_argument("--s3-output", required=True, help="S3 报告输出路径")
    parser.add_argument("--databricks-host", required=True, help="Databricks workspace URL")
    parser.add_argument("--databricks-secret-arn", default=None,
                        help="AWS Secrets Manager ARN（OAuth2 认证，推荐）")
    parser.add_argument("--databricks-token", default=None,
                        help="Databricks PAT（备选，secret-arn 优先）")
    parser.add_argument("--workers", type=int, default=15, help="并行 worker 数（默认 15）")
    parser.add_argument("--timeout", type=int, default=600, help="单表超时秒数（默认 600）")
    parser.add_argument("--application-id", default=EMR_SERVERLESS_APP_ID,
                        help="EMR Serverless 应用 ID")
    parser.add_argument("--execution-role-arn", default=EXECUTION_ROLE_ARN,
                        help="EMR Serverless 执行角色 ARN")
    args = parser.parse_args()

    session = Session(
        application_id=args.application_id,
        jobtype=1,
        region=REGION,
        job_role=args.execution_role_arn,
        logs_s3_path=S3_LOGS_PATH,
        script_s3_path=S3_SCRIPTS_PATH,
        spark_conf=build_spark_conf(args),
    )

    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbx_diff.py")
    job_args = build_job_args(args)

    print(f"应用 ID: {args.application_id}")
    print(f"输入 CSV: {args.csv}")
    print(f"输出报告: {args.s3_output}")
    print(f"认证方式: {'OAuth2 (secret-arn)' if args.databricks_secret_arn else 'Token'}")

    result = session.submit_file("dbx_diff", script_path, args=job_args)

    if result.status in ("SUCCESS", "COMPLETED"):
        print(f"\n作业成功完成。报告路径: {args.s3_output}")
        sys.exit(0)
    else:
        print(f"\n作业失败，状态: {result.status}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
