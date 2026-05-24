"""
Submit dbx_diff.py for 5 new test tables via emr_common.Session.
Uses workspace.demo2, pt range 20260524-20260526.
Tests the --widget parameter rename.
"""

import sys
import json
sys.path.insert(0, '/home/ec2-user/verson5/dbx-diff')
from emr_common import Session

TABLES = [
    "workspace.demo2.v5_t2_part_match",
    "workspace.demo2.v5_t2_part_mismatch",
    "workspace.demo2.v5_t2_nopart_str",
    "workspace.demo2.v5_t2_nopart_num_diff",
    "workspace.demo2.v5_t2_part_count_only",
]

COMMON_WIDGET = {
    "iceberg-output": "workspace.demo2.verify_result",
    "databricks-host": "https://dbc-51ad87e6-c26d.cloud.databricks.com",
    "databricks-secret-arn": "arn:aws:secretsmanager:us-west-2:785682719467:secret:databricks-6lajvp",
    "pt-start": "20260524",
    "pt-end": "20260526",
    "region": "us-west-2",
    "task-id": "v5_5t_test",
    "instance-id": "run_001",
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

        widget = dict(COMMON_WIDGET)
        widget["table-name"] = table

        args = ["--widget", json.dumps(widget)]

        try:
            result = session.submit_file(f"5t_test_{table.split('.')[-1]}", script_path, args=args)
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

    expected = {
        "v5_t2_part_match": "PASS (all partitions match)",
        "v5_t2_part_mismatch": "FAIL (pt=25 cost differs: DBX=888, EMR=44)",
        "v5_t2_nopart_str": "PASS (count match, no numeric)",
        "v5_t2_nopart_num_diff": "FAIL (count differs: DBX=5, EMR=4)",
        "v5_t2_part_count_only": "FAIL (pt=26 count: DBX=4, EMR=3)",
    }
    print(f"\nEXPECTED RESULTS:")
    for t, exp in expected.items():
        print(f"  {t:30s} {exp}")


if __name__ == "__main__":
    main()
