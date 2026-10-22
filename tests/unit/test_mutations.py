import sqlite3

import pytest

from ephemeraldb.core.errors import TransactionConflictError
from ephemeraldb.schema.generator import generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.schema.mutations import apply_entity_delta


def test_apply_entity_delta_atomic_success(tmp_path) -> None:  # type: ignore[no-untyped-def]
    database_path = _generated_database(tmp_path)
    connection = sqlite3.connect(database_path)
    try:
        entity_id, numeric_value, version = connection.execute(
            "SELECT entity_id, numeric_value, version FROM entities ORDER BY entity_id LIMIT 1"
        ).fetchone()

        apply_entity_delta(
            connection,
            transaction_id="transaction-1",
            entity_id=entity_id,
            delta=7,
            expected_version=version,
        )

        updated_numeric_value, updated_version = connection.execute(
            "SELECT numeric_value, version FROM entities WHERE entity_id = ?",
            (entity_id,),
        ).fetchone()
        assert updated_numeric_value == numeric_value + 7
        assert updated_version == version + 1
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM entity_versions WHERE entity_id = ? AND version = ?",
                (entity_id, version + 1),
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT status FROM transactions WHERE transaction_id = 'transaction-1'"
            ).fetchone()[0]
            == "COMMITTED"
        )
        assert (
            connection.execute(
                "SELECT delta FROM transaction_items WHERE transaction_id = 'transaction-1'"
            ).fetchone()[0]
            == 7
        )
        assert (
            connection.execute(
                "SELECT operation FROM audit_log WHERE transaction_id = 'transaction-1'"
            ).fetchone()[0]
            == "ENTITY_UPDATED"
        )
    finally:
        connection.close()


def test_apply_entity_delta_conflict_rolls_back(tmp_path) -> None:  # type: ignore[no-untyped-def]
    database_path = _generated_database(tmp_path)
    connection = sqlite3.connect(database_path)
    try:
        entity_id, numeric_value, version = connection.execute(
            "SELECT entity_id, numeric_value, version FROM entities ORDER BY entity_id LIMIT 1"
        ).fetchone()

        with pytest.raises(TransactionConflictError):
            apply_entity_delta(
                connection,
                transaction_id="transaction-conflict",
                entity_id=entity_id,
                delta=10,
                expected_version=version + 100,
            )

        assert (
            connection.execute(
                "SELECT numeric_value, version FROM entities WHERE entity_id = ?",
                (entity_id,),
            ).fetchone()
            == (numeric_value, version)
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM transactions WHERE transaction_id = 'transaction-conflict'"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM transaction_items
                WHERE transaction_id = 'transaction-conflict'
                """
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM audit_log WHERE transaction_id = 'transaction-conflict'"
            ).fetchone()[0]
            == 0
        )
    finally:
        connection.close()


def test_apply_entity_delta_rolls_back_mid_transaction_failure(tmp_path) -> None:  # type: ignore[no-untyped-def]
    database_path = _generated_database(tmp_path)
    connection = sqlite3.connect(database_path)
    try:
        entity_id, numeric_value, version = connection.execute(
            "SELECT entity_id, numeric_value, version FROM entities ORDER BY entity_id LIMIT 1"
        ).fetchone()

        def fail() -> None:
            raise RuntimeError("injected failure")

        with pytest.raises(RuntimeError, match="injected failure"):
            apply_entity_delta(
                connection,
                transaction_id="transaction-rollback",
                entity_id=entity_id,
                delta=10,
                expected_version=version,
                after_entity_update_hook=fail,
            )

        assert (
            connection.execute(
                "SELECT numeric_value, version FROM entities WHERE entity_id = ?",
                (entity_id,),
            ).fetchone()
            == (numeric_value, version)
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM transactions WHERE transaction_id = 'transaction-rollback'"
            ).fetchone()[0]
            == 0
        )
    finally:
        connection.close()


def _generated_database(tmp_path) -> object:  # type: ignore[no-untyped-def]
    dataset = generate_dataset(
        tmp_path / "dataset",
        DatasetConfig(tenant_count=1, entities_per_tenant=5),
    )
    return dataset.output_dir / "tenant-000001.db"
