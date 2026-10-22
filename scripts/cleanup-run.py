"""Preview or explicitly remove one console run's retained AWS objects after writers stop."""
from __future__ import annotations

import argparse
import json
import os
import re


def main() -> None:
    import boto3
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--writers-stopped", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-f0-9]{32}", args.run_id):
        raise SystemExit("Expected one 32-character console run ID")
    if args.execute and not args.writers_stopped:
        raise SystemExit("Stop every writer first, then explicitly pass --writers-stopped")
    client = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"),
                           region_name=os.environ.get("AWS_REGION", "ap-south-1")).client("s3")
    for bucket_env, prefix_env, default in [("S3_EXPRESS_BUCKET", "S3_EXPRESS_PREFIX", "wal"),
                                           ("S3_CHECKPOINT_BUCKET", "S3_CHECKPOINT_PREFIX", "checkpoints")]:
        bucket = os.environ[bucket_env]
        prefix = f"{os.environ.get(prefix_env, default).strip('/')}/console/{args.run_id}/"
        keys = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
            if len(keys) > 50000:
                raise SystemExit("Object budget exceeded; no deletes issued for this bucket")
        print(json.dumps({"bucket": bucket, "prefix": prefix, "objects": len(keys),
                          "mode": "DELETE" if args.execute else "PREVIEW", "aws_requests_are_chargeable": True}))
        if args.execute:
            for start in range(0, len(keys), 1000):
                result = client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": key} for key in keys[start:start+1000]], "Quiet": True})
                if result.get("Errors"):
                    raise SystemExit("Some deletes failed; rerun preview and investigate")


if __name__ == "__main__":
    main()
