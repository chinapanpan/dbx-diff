"""
Submit dbx_diff.py for all 9 test tables via emr_common.Session.
Uses workspace.demo2, pt range 20260521-20260523.
"""

import sys
import json
sys.path.insert(0, '/home/ec2-user/verson5/dbx-diff')
from emr_common import Session

TABLES = [
    "workspace.demo2.v5_t_all_match",
    "workspace.demo2.v5_t_part_diff",
    "workspace.demo2.v5_t_nopart_match",
    "workspace.demo2.v5_t_nopart_diff",
    "workspace.demo2.v5_t_count_only_part",
    "workspace.demo2.v5_t_count_only_nopart",
    "workspace.demo2.v5_t_null_values",
    "workspace.demo2.v5_t_multi_types",
    "workspace.demo2.v5_t_missing_pt",
]

COMMON_WIDGETS = {
    "iceberg-output": "workspace.demo2.verify_result",
    "databricks-host": "https://dbc-51ad87e6-c26d.cloud.databricks.com",
    "databricks-secret-arn": "arn:aws:secretsmanager:us-west-2:785682719467:secret:databricks-6lajvp",
    "pt-start": "20260521",
    "pt-end": "20260523",
    "region": "us-west-2",
    "task-id": "v5_full_test",
    "instance-id": "run_002",
    "attemp-id": "1",
}


def main():
    session = Session(
        application_id='00g5t48pdtcnid0l',
        jobtype=1,
        region='us-west-2',
        job_role='arn:aws:iam::785682719467:role/EMRServerlessExecutionRole',
        logs_s3_path='s3://zpf-databricks-event/logs/',
        script_s3_path='s3://zpf-databricks-event/code/',
        spark_conf='--conf spark.dynamicAllocation.enabled=true'
    )

    script_path = '/home/ec2-user/verson5/dbx-diff/dbx_diff.py'
    results = []

    for i, table in enumerate(TABLES):
        print(f"\n{'='*60}")
        print(f"[{i+1}/{len(TABLES)}] Testing: {table}")
        print(f"{'='*60}")

        widgets = dict(COMMON_WIDGETS)
        widgets["table-name"] = table

        args = ["--widget", json.dumps(widgets)]

        try:
            result = session.submit_file(f"v5_test_{table.split('.')[-1]}", script_path, args=args)
            results.append((table, result.status, result.job_run_id))
            print(f"  => {result.status} (job: {result.job_run_id})")
        except Exception as e:
            results.append((table, "ERROR", str(e)[:80]))
            print(f"  => ERROR: {e}")

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for table, status, job_id in results:
        tname = table.split('.')[-1]
        print(f"  {tname:30s} {status:10s} {job_id}")


if __name__ == "__main__":
    main()
