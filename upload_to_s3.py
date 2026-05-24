"""
上传本地文件到 S3。

Usage:
  python3 upload_to_s3.py --filepath /path/to/local/file --s3location s3://bucket/key/path
"""

import argparse
import sys
import os

import boto3
from botocore.exceptions import ClientError


def parse_s3_path(s3_path: str):
    """解析 S3 路径为 bucket 和 key。"""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"S3 路径必须以 s3:// 开头: {s3_path}")
    path = s3_path[5:]
    parts = path.split("/", 1)
    if len(parts) < 2 or not parts[1]:
        raise ValueError(f"S3 路径必须包含 bucket 和 key: {s3_path}")
    return parts[0], parts[1]


def main():
    parser = argparse.ArgumentParser(description="上传本地文件到 S3")
    parser.add_argument("--filepath", required=True, help="本地文件路径")
    parser.add_argument("--s3location", required=True, help="S3 目标路径，如 s3://bucket/key/file.txt")
    args = parser.parse_args()

    if not os.path.isfile(args.filepath):
        print(f"ERROR: 文件不存在: {args.filepath}", file=sys.stderr)
        sys.exit(1)

    bucket, key = parse_s3_path(args.s3location)

    file_size = os.path.getsize(args.filepath)
    print(f"文件: {args.filepath} ({file_size} bytes)")
    print(f"目标: s3://{bucket}/{key}")

    s3 = boto3.client("s3")
    try:
        s3.upload_file(args.filepath, bucket, key)
    except ClientError as e:
        print(f"ERROR: 上传失败: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"上传成功: {args.filepath} -> s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
