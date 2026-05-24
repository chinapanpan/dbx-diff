"""
基于 emr_common.Session 提交 dbx_diff.py 作业到 EMR Serverless (v5 - Scheduler Edition).

所有参数通过 --widget JSON 传递给 dbx_diff.py。

Usage:
  python3 submit_job.py \
    --table-name workspace.demo2.test_nopk_part \
    --iceberg-output workspace.demo2.verify_result \
    --databricks-host https://dbc-51ad87e6-c26d.cloud.databricks.com \
    --databricks-secret-arn arn:aws:secretsmanager:us-west-2:785682719467:secret:databricks-6lajvp \
    --pt-start 20260521 \
    --pt-end 20260522 \
    --task-id task001 \
    --instance-id inst001 \
    --attemp-id att001
"""

import argparse
import sys
import os
import json

from emr_common import Session


EMR_SERVERLESS_APP_ID = "00g5t48pdtcnid0l"
EXECUTION_ROLE_ARN = "arn:aws:iam::785682719467:role/EMRServerlessExecutionRole"
S3_LOGS_PATH = "s3://zpf-databricks-event/logs/"
S3_SCRIPTS_PATH = "s3://zpf-databricks-event/code/"
REGION = "us-west-2"


def build_spark_conf(args) -> str:
    """构建运行时需要的环境变量配置（仅 Databricks 凭证相关）。"""
    confs = [
        f"--conf spark.emr-serverless.driverEnv.DATABRICKS_HOST={args.databricks_host}",
    ]
    return " ".join(confs)


def build_widget_json(args) -> str:
    """构建 --widget 所需的 JSON 字符串。"""
    widget = {
        "table-name": args.table_name,
        "iceberg-output": args.iceberg_output,
        "databricks-host": args.databricks_host,
        "databricks-secret-arn": args.databricks_secret_arn,
        "pt-start": args.pt_start,
        "pt-end": args.pt_end,
        "workers": str(args.workers),
        "timeout": str(args.timeout),
        "region": args.region,
        "task-id": args.task_id,
        "instance-id": args.instance_id,
        "attemp-id": args.attemp_id,
    }
    return json.dumps(widget)


def main():
    parser = argparse.ArgumentParser(description="提交 dbx_diff 数据对比作业 (v5 - Scheduler)")
    parser.add_argument("--table-name", required=True,
                        help="待比较表名，全路径：catalog.db.tablename")
    parser.add_argument("--iceberg-output", required=True,
                        help="结果写入的 Iceberg 表名（全路径）")
    parser.add_argument("--databricks-host", required=True,
                        help="Databricks workspace URL")
    parser.add_argument("--databricks-secret-arn", required=True,
                        help="AWS Secrets Manager ARN（OAuth2 认证）")
    parser.add_argument("--pt-start", required=True,
                        help="分区起始值（含），如 20260521")
    parser.add_argument("--pt-end", required=True,
                        help="分区结束值（含），如 20260522")
    parser.add_argument("--task-id", required=True,
                        help="任务 ID（写入 Iceberg 结果表）")
    parser.add_argument("--instance-id", required=True,
                        help="实例 ID（写入 Iceberg 结果表）")
    parser.add_argument("--attemp-id", required=True,
                        help="尝试 ID（写入 Iceberg 结果表）")
    parser.add_argument("--workers", type=int, default=15,
                        help="并行 worker 数（默认 15）")
    parser.add_argument("--timeout", type=int, default=1800,
                        help="单表超时秒数（默认 1800）")
    parser.add_argument("--region", default="us-west-2",
                        help="AWS Secrets Manager 区域（默认 us-west-2）")
    parser.add_argument("--application-id", default=EMR_SERVERLESS_APP_ID,
                        help="EMR Serverless 应用 ID")
    parser.add_argument("--execution-role-arn", default=EXECUTION_ROLE_ARN,
                        help="EMR Serverless 执行角色 ARN")
    args = parser.parse_args()

    widget_json = build_widget_json(args)

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
    job_args = ["--widget", widget_json]

    print(f"应用 ID: {args.application_id}")
    print(f"表名: {args.table_name}")
    print(f"Iceberg 输出表: {args.iceberg_output}")
    print(f"认证方式: OAuth2 (secret-arn)")
    print(f"分区范围: pt >= {args.pt_start} AND pt < {int(args.pt_end) + 1}")
    print(f"Task ID: {args.task_id}")
    print(f"Instance ID: {args.instance_id}")
    print(f"Attemp ID: {args.attemp_id}")
    print(f"Widget JSON: {widget_json}")

    result = session.submit_file("dbx_diff", script_path, args=job_args)

    if result.status in ("SUCCESS", "COMPLETED"):
        print(f"\n作业成功完成。结果已写入: {args.iceberg_output}")
        sys.exit(0)
    else:
        print(f"\n作业失败，状态: {result.status}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
