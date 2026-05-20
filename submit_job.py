"""
Submit dbx_diff.py as a Spark job on EMR (YARN or EMR Serverless).
"""

import argparse
import subprocess
import sys
import os


def submit_yarn(csv_path: str, s3_output: str, workers: int, timeout: int):
    """Submit via spark-submit on YARN (EMR cluster mode)."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbx_diff_path = os.path.join(script_dir, "dbx_diff.py")

    cmd = [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", "spark.executor.memory=4g",
        "--conf", "spark.executor.cores=2",
        "--conf", "spark.dynamicAllocation.enabled=true",
        "--conf", "spark.dynamicAllocation.maxExecutors=20",
        "--packages", "io.delta:delta-spark_2.12:3.3.0",
        dbx_diff_path,
        "--csv", csv_path,
        "--s3-output", s3_output,
        "--workers", str(workers),
        "--timeout", str(timeout),
    ]

    print(f"Submitting spark job: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False, text=True)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Submit DbxDiff Spark Job")
    parser.add_argument("--csv", required=True, help="Path to input CSV file")
    parser.add_argument("--s3-output", required=True, help="S3 output path for report")
    parser.add_argument("--workers", type=int, default=15, help="Parallel workers")
    parser.add_argument("--timeout", type=int, default=600, help="Timeout per table (seconds)")
    parser.add_argument("--mode", choices=["yarn", "local"], default="yarn", help="Submission mode")
    args = parser.parse_args()

    if args.mode == "yarn":
        rc = submit_yarn(args.csv, args.s3_output, args.workers, args.timeout)
    else:
        # Local mode for testing
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cmd = [
            sys.executable, os.path.join(script_dir, "dbx_diff.py"),
            "--csv", args.csv,
            "--s3-output", args.s3_output,
            "--workers", str(args.workers),
            "--timeout", str(args.timeout),
        ]
        print(f"Running locally: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=False, text=True)
        rc = result.returncode

    sys.exit(rc)


if __name__ == "__main__":
    main()
