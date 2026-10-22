"""Small sequential S3 Express PUT latency probe."""

from __future__ import annotations

import argparse
import json
import statistics
import time
import uuid

from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore
from ephemeraldb.config.settings import load_settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe raw S3 Express PUT latency.")
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--requests", type=int, default=10)
    args = parser.parse_args(argv)
    if args.payload_bytes < 1:
        parser.error("--payload-bytes must be >= 1")
    if not 1 <= args.requests <= 100:
        parser.error("--requests must be between 1 and 100")

    settings = load_settings()
    config = S3ExpressWalConfig.from_settings(settings)
    store = S3ExpressWalStore(config)
    run_id = uuid.uuid4().hex
    payload = b"x" * args.payload_bytes
    latencies: list[int] = []
    keys: list[str] = []
    prefix = f"{config.prefix}/probe/{run_id}"

    print(
        json.dumps(
            {
                "probe": "s3-express-put-latency",
                "bucket": config.bucket,
                "region": config.region,
                "prefix": prefix,
                "requests": args.requests,
                "payload_bytes": args.payload_bytes,
                "expected_puts": args.requests,
                "expected_deletes": args.requests,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )

    for index in range(args.requests):
        key = f"{prefix}/{index:04d}.bin"
        started_at = time.perf_counter_ns()
        store._client.put_object(
            Bucket=config.bucket,
            Key=key,
            Body=payload,
            StorageClass="EXPRESS_ONEZONE",
            IfNoneMatch="*",
        )
        finished_at = time.perf_counter_ns()
        latencies.append(finished_at - started_at)
        keys.append(key)

    for key in keys:
        store._client.delete_object(Bucket=config.bucket, Key=key)

    print(json.dumps(_summary(latencies), ensure_ascii=True, indent=2, sort_keys=True))
    return 0


def _summary(latencies_ns: list[int]) -> dict[str, float | int]:
    sorted_values = sorted(latencies_ns)
    return {
        "count": len(sorted_values),
        "min_ns": min(sorted_values),
        "mean_ns": statistics.fmean(sorted_values),
        "p50_ns": _percentile(sorted_values, 50),
        "p95_ns": _percentile(sorted_values, 95),
        "p99_ns": _percentile(sorted_values, 99),
        "max_ns": max(sorted_values),
    }


def _percentile(sorted_values: list[int], percentile: int) -> int:
    index = min(len(sorted_values) - 1, round((percentile / 100) * (len(sorted_values) - 1)))
    return sorted_values[index]


if __name__ == "__main__":
    raise SystemExit(main())
