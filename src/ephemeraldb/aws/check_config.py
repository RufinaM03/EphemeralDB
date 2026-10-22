"""Validate local S3 Express WAL configuration without creating resources."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from ephemeraldb.aws.s3_express import S3ExpressWalConfig, S3ExpressWalStore
from ephemeraldb.config.settings import load_settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check S3 Express WAL configuration.")
    parser.parse_args(argv)
    settings = load_settings()
    config = S3ExpressWalConfig.from_settings(settings)
    store = S3ExpressWalStore(config)
    store.check_bucket_reachable()
    print(
        json.dumps(
            {
                "ok": True,
                "bucket": config.bucket,
                "region": config.region,
                "zone_id": config.zone_id,
                "prefix": config.prefix,
                "retry_mode": config.retry_mode,
                "max_attempts": config.max_attempts,
                "configuration": asdict(config),
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
