"""
基于 emr_common.Session 提交 dbx_diff.py 作业到 EMR Serverless。

Spark 相关配置（Iceberg catalog、venv、动态分配等）已在 EMR Serverless Application 级别配置，
本脚本仅需传入业务参数。

Usage:
  python3 submit_job.py \
    --csv tables.csv \
    --s3-output s3://zpf-databricks-event/reports/diff_report.md \
    --databricks-host https://dbc-51ad87e6-c26d.cloud.databricks.com \
    --databricks-secret-arn arn:aws:secretsmanager:us-west-2:<account>:secret:<name> \
    --pt-start 20260521 \
    --pt-end 20260522
"""

import argparse
import sys
import os
from datetime import datetime

import boto3

from emr_common import Session


EMR_SERVERLESS_APP_ID = "00g5t48pdtcnid0l"
EXECUTION_ROLE_ARN = "arn:aws:iam::785682719467:role/EMRServerlessExecutionRole"
S3_LOGS_PATH = "s3://zpf-databricks-event/logs/"
S3_SCRIPTS_PATH = "s3://zpf-databricks-event/code/"
REGION = "us-west-2"


def upload_csv_to_s3(local_csv: str) -> str:
    """上传本地 CSV 到 S3，返回 S3 路径。"""
    filename = os.path.basename(local_csv)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    s3_bucket = S3_SCRIPTS_PATH.split("/")[2]
    s3_key_prefix = "/".join(S3_SCRIPTS_PATH.split("/")[3:])
    s3_key = f"{s3_key_prefix}{timestamp}_{filename}"

    s3 = boto3.client("s3")
    s3.upload_file(local_csv, s3_bucket, s3_key)
    s3_path = f"s3://{s3_bucket}/{s3_key}"
    print(f"已上传 CSV: {local_csv} -> {s3_path}")
    return s3_path


def delete_s3_file(s3_path: str):
    """删除 S3 上的文件。"""
    s3 = boto3.client("s3")
    bucket = s3_path.split("/")[2]
    key = "/".join(s3_path.split("/")[3:])
    s3.delete_object(Bucket=bucket, Key=key)
    print(f"已清除 S3 临时文件: {s3_path}")


def build_spark_conf(args) -> str:
    """构建运行时需要的环境变量配置（仅 Databricks 凭证相关）。"""
    confs = [
        f"--conf spark.emr-serverless.driverEnv.DATABRICKS_HOST={args.databricks_host}",
    ]
    return " ".join(confs)


def build_job_args(args, s3_csv: str) -> list:
    """构建 dbx_diff.py 的入口参数。"""
    job_args = [
        "--csv", s3_csv,
        "--s3-output", args.s3_output,
        "--workers", str(args.workers),
        "--timeout", str(args.timeout),
        "--databricks-host", args.databricks_host,
        "--databricks-secret-arn", args.databricks_secret_arn,
    ]
    if args.pt_start:
        job_args.extend(["--pt-start", args.pt_start])
    if args.pt_end:
        job_args.extend(["--pt-end", args.pt_end])
    return job_args


def main():
    parser = argparse.ArgumentParser(description="提交 dbx_diff 数据对比作业")
    parser.add_argument("--csv", required=True, help="输入 CSV 路径（本地文件）")
    parser.add_argument("--s3-output", required=True, help="S3 报告输出路径")
    parser.add_argument("--databricks-host", required=True, help="Databricks workspace URL")
    parser.add_argument("--databricks-secret-arn", required=True,
                        help="AWS Secrets Manager ARN（OAuth2 认证）")
    parser.add_argument("--pt-start", default=None, help="分区起始值（含），如 20260521")
    parser.add_argument("--pt-end", default=None, help="分区结束值（含，过滤时用 < end+1），如 20260522")
    parser.add_argument("--workers", type=int, default=15, help="并行 worker 数（默认 15）")
    parser.add_argument("--timeout", type=int, default=600, help="单表超时秒数（默认 600）")
    parser.add_argument("--application-id", default=EMR_SERVERLESS_APP_ID,
                        help="EMR Serverless 应用 ID")
    parser.add_argument("--execution-role-arn", default=EXECUTION_ROLE_ARN,
                        help="EMR Serverless 执行角色 ARN")
    args = parser.parse_args()

    s3_csv = upload_csv_to_s3(args.csv)

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
    job_args = build_job_args(args, s3_csv)

    print(f"应用 ID: {args.application_id}")
    print(f"输入 CSV: {args.csv} -> {s3_csv}")
    print(f"输出报告: {args.s3_output}")
    print(f"认证方式: OAuth2 (secret-arn)")
    if args.pt_start and args.pt_end:
        print(f"分区范围: pt >= {args.pt_start} AND pt < {int(args.pt_end) + 1}")

    result = session.submit_file("dbx_diff", script_path, args=job_args)

    delete_s3_file(s3_csv)

    if result.status in ("SUCCESS", "COMPLETED"):
        print(f"\n作业成功完成。报告路径: {args.s3_output}")
        sys.exit(0)
    else:
        print(f"\n作业失败，状态: {result.status}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
