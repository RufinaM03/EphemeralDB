import sqlite3

from ephemeraldb.schema.generator import generate_dataset
from ephemeraldb.schema.models import DatasetConfig
from ephemeraldb.schema.queries import QUERY_CATALOG


def test_query_catalog_sql_is_sqlite_compatible(tmp_path) -> None:  # type: ignore[no-untyped-def]
    dataset = generate_dataset(
        tmp_path / "dataset",
        DatasetConfig(tenant_count=1, entities_per_tenant=5),
    )
    database_path = dataset.output_dir / "tenant-000001.db"
    connection = sqlite3.connect(database_path)
    try:
        tenant_id, entity_id, state = connection.execute(
            "SELECT tenant_id, entity_id, state FROM entities ORDER BY entity_id LIMIT 1"
        ).fetchone()
        params = {
            "tenant_id": tenant_id,
            "entity_id": entity_id,
            "state": state,
            "limit": 10,
            "metadata_key": "attribute_00",
            "source_state": state,
            "relationship_type": "RELATED",
        }
        for sql in QUERY_CATALOG.values():
            connection.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    finally:
        connection.close()
