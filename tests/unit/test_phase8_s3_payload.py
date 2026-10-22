from __future__ import annotations

from dataclasses import replace
from io import BytesIO

from test_wal_batching import _record

from ephemeraldb.aws.s3_express import S3ExpressWalConfig
from ephemeraldb.ownership.s3_payload import FencedS3ExpressWalStore
from ephemeraldb.wal.checksum import attach_record_checksum


class _Client:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, **request: object) -> dict[str, str]:
        key = str(request["Key"])
        if key in self.objects:
            raise RuntimeError("test does not overwrite candidates")
        body = request["Body"]
        assert isinstance(body, bytes)
        self.objects[key] = body
        return {"ETag": '"opaque-etag"'}

    def get_object(self, **request: object) -> dict[str, object]:
        return {"Body": BytesIO(self.objects[str(request["Key"])])}


def test_s3_express_fenced_single_candidates_do_not_collide_at_same_sequence() -> None:
    client = _Client()
    store = FencedS3ExpressWalStore(
        S3ExpressWalConfig(bucket="example--zone--x-s3", prefix="phase8"),
        client=client,
    )
    first = attach_record_checksum(replace(_record("tenant-a", 1), owner_epoch=1))
    second = attach_record_checksum(replace(_record("tenant-a", 1), owner_epoch=2))
    first_receipt = store.persist_record(first)
    second_receipt = store.persist_record(second)
    assert first_receipt.key != second_receipt.key
    assert store.fetch_record_by_key(first_receipt.key) == first
    assert store.fetch_record_by_key(second_receipt.key) == second
