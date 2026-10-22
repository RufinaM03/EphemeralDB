"""Read-only resource inventory. Never deletes resources or claims an exhaustive bill audit."""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def inventory(session: Any, project: str) -> dict[str, Any]:
    resources: list[dict[str, Any]] = []
    failures: list[str] = []
    checks = [
        ("tagged-resources", "resourcegroupstaggingapi", "get_resources", {"TagFilters": [{"Key": "Project", "Values": [project]}]}, "ResourceTagMappingList"),
        ("instances", "ec2", "describe_instances", {"Filters": [{"Name": "tag:Project", "Values": [project]}, {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}]}, "Reservations"),
    ]
    for label, service, operation, arguments, key in checks:
        try:
            client = session.client(service)
            paginator = client.get_paginator(operation)
            for page in paginator.paginate(**arguments):
                resources.extend({"kind": label, "resource": resource} for resource in page.get(key, []))
        except Exception as exc:
            failures.append(f"{label}: {type(exc).__name__}")
    return {"region": session.region_name, "project": project, "resources": resources,
            "errors": failures, "complete": not failures,
            "limitations": "Tagged regional resources only. Review Billing for untagged resources, retained S3 data, snapshots, public IPv4, logs and delayed charges."}


def main() -> int:
    import boto3
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile")
    parser.add_argument("--region", default="ap-south-1")
    parser.add_argument("--project", default="ephemeraldb")
    args = parser.parse_args()
    result = inventory(boto3.Session(profile_name=args.profile, region_name=args.region), args.project)
    print(json.dumps(result, indent=2, default=str))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
