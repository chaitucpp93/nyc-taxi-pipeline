"""Land one file in S3: skip if present, download safely, upload, verify size."""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()  # must run before boto3 creates a client

import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

from download import ZONE_URL, download  # noqa: E402

log = logging.getLogger("land")


class UploadVerificationError(Exception):
    """S3 holds a different number of bytes than the local file."""


def exists_in_s3(s3: Any, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise  # 403 and others are permission/config problems, not "missing"


def land(s3: Any, bucket: str, url: str, key: str, local_dir: Path = Path("data")) -> bool:
    """Return True if uploaded, False if skipped."""
    if exists_in_s3(s3, bucket, key):
        log.info("skip (already in S3): s3://%s/%s", bucket, key)
        return False

    local = local_dir / Path(key).name
    download(url, local)  # your safe download; skips if the local copy exists

    size = local.stat().st_size
    log.info("uploading %s -> s3://%s/%s", local, bucket, key)
    s3.upload_file(str(local), bucket, key)

    remote_size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if remote_size != size:
        s3.delete_object(Bucket=bucket, Key=key)  # never leave a bad object that later runs would skip
        raise UploadVerificationError(f"{key}: local {size} bytes, S3 {remote_size} bytes")

    log.info("landed s3://%s/%s (%d bytes)", bucket, key, size)
    return True

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"


def trip_key(month: str) -> str:
    year, mm = month.split("-")
    return f"raw/yellow/year={year}/month={mm}/yellow_tripdata_{month}.parquet"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise SystemExit("S3_BUCKET is not set. Copy .env.example to .env and fill it in.")
    s3 = boto3.client("s3")
    land(s3, bucket, ZONE_URL, "raw/reference/taxi_zone_lookup.csv")
    for month in ["2025-01", "2025-02", "2025-03"]:
        land(s3, bucket, f"{BASE_URL}/trip-data/yellow_tripdata_{month}.parquet", trip_key(month))

if __name__ == "__main__":
    main()