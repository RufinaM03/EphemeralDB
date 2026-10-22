from __future__ import annotations

from io import BytesIO

from ephemeraldb.config.settings import load_settings
from ephemeraldb.ownership.s3_store import S3AuthorityStore
from ephemeraldb.ownership.store import CasResult


class _Body(BytesIO):
    pass


class _ClientError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class _S3Client:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.generation = 0
        self.lose_response = False

    def put_object(self, **request: object) -> dict[str, str]:
        key = str(request["Key"])
        body = request["Body"]
        assert isinstance(body, bytes)
        current = self.objects.get(key)
        if request.get("IfNoneMatch") == "*" and current is not None:
            raise _ClientError("PreconditionFailed")
        if "IfMatch" in request and (
            current is None or current[1] != request["IfMatch"]
        ):
            raise _ClientError("PreconditionFailed")
        self.generation += 1
        etag = f'"etag-{self.generation}"'
        self.objects[key] = (body, etag)
        if self.lose_response:
            self.lose_response = False
            raise TimeoutError("response lost after service apply")
        return {"ETag": etag}

    def get_object(self, **request: object) -> dict[str, object]:
        key = str(request["Key"])
        try:
            payload, etag = self.objects[key]
        except KeyError as exc:
            raise _ClientError("NoSuchKey") from exc
        return {"Body": _Body(payload), "ETag": etag}


def _store(client: _S3Client) -> S3AuthorityStore:
    settings = load_settings(
        {
            "S3_CHECKPOINT_BUCKET": "general-purpose-bucket",
            "S3_CHECKPOINT_PREFIX": "phase8-tests",
            "S3_EXPRESS_BUCKET": "express--zone--x-s3",
        }
    )
    return S3AuthorityStore(settings, client=client)


def test_s3_authority_uses_conditional_create_and_opaque_etag_cas() -> None:
    store = _store(_S3Client())
    assert store.create_control("control.json", b"one") is CasResult.APPLIED
    assert store.create_control("control.json", b"overwrite") is CasResult.CONFLICT
    snapshot = store.get_control("control.json")
    assert snapshot.payload == b"one"
    assert store.compare_and_swap("control.json", '"wrong"', b"bad") is CasResult.CONFLICT
    assert store.compare_and_swap("control.json", snapshot.validator, b"two") is CasResult.APPLIED
    assert store.get_control("control.json").payload == b"two"


def test_s3_authority_classifies_response_loss_unknown_and_immutable_retry() -> None:
    client = _S3Client()
    store = _store(client)
    assert store.create_control("control.json", b"one") is CasResult.APPLIED
    snapshot = store.get_control("control.json")
    client.lose_response = True
    assert store.compare_and_swap("control.json", snapshot.validator, b"two") is CasResult.UNKNOWN
    assert store.get_control("control.json").payload == b"two"
    store.put_immutable("descriptor.json", b"descriptor")
    store.put_immutable("descriptor.json", b"descriptor")
