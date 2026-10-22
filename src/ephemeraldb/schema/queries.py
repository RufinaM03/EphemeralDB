"""SQLite-compatible query catalog for future benchmark phases."""

from __future__ import annotations

QUERY_CATALOG: dict[str, str] = {
    "Q1_ENTITY_LOOKUP": """
        SELECT entity_id, tenant_id, state, numeric_value, version, created_at, updated_at
        FROM entities
        WHERE tenant_id = :tenant_id
          AND entity_id = :entity_id
    """,
    "Q2_STATE_SCAN": """
        SELECT entity_id, state, numeric_value, version, updated_at
        FROM entities
        WHERE tenant_id = :tenant_id
          AND state = :state
        ORDER BY updated_at, entity_id
        LIMIT :limit
    """,
    "Q3_RELATIONSHIP_EXPANSION": """
        SELECT
            r.source_entity_id,
            r.target_entity_id,
            r.relationship_type,
            r.weight,
            target.state AS target_state,
            target.numeric_value AS target_numeric_value,
            target.version AS target_version
        FROM relationships AS r
        JOIN entities AS target
          ON target.tenant_id = r.tenant_id
         AND target.entity_id = r.target_entity_id
        WHERE r.tenant_id = :tenant_id
          AND r.source_entity_id = :entity_id
        ORDER BY r.relationship_type, r.target_entity_id
        LIMIT :limit
    """,
    "Q4_METADATA_LOOKUP": """
        SELECT metadata_key, metadata_value, updated_at
        FROM metadata
        WHERE tenant_id = :tenant_id
          AND entity_id = :entity_id
        ORDER BY metadata_key
    """,
    "Q5_ENTITY_HISTORY": """
        SELECT entity_id, version, state, numeric_value, created_at
        FROM entity_versions
        WHERE tenant_id = :tenant_id
          AND entity_id = :entity_id
        ORDER BY version DESC
        LIMIT :limit
    """,
    "Q6_COMPLEX_JOIN": """
        SELECT
            source.entity_id AS source_entity_id,
            source.state AS source_state,
            r.relationship_type,
            r.weight,
            target.entity_id AS target_entity_id,
            target.state AS target_state,
            target.numeric_value AS target_numeric_value,
            m.metadata_key,
            m.metadata_value
        FROM entities AS source
        JOIN relationships AS r
          ON r.tenant_id = source.tenant_id
         AND r.source_entity_id = source.entity_id
        JOIN entities AS target
          ON target.tenant_id = r.tenant_id
         AND target.entity_id = r.target_entity_id
        LEFT JOIN metadata AS m
          ON m.tenant_id = target.tenant_id
         AND m.entity_id = target.entity_id
         AND m.metadata_key = :metadata_key
        WHERE source.tenant_id = :tenant_id
          AND source.state = :source_state
          AND r.relationship_type = :relationship_type
        ORDER BY source.entity_id, target.entity_id
        LIMIT :limit
    """,
    "Q7_AGGREGATE": """
        SELECT state, COUNT(*) AS entity_count
        FROM entities
        WHERE tenant_id = :tenant_id
        GROUP BY state
        ORDER BY state
    """,
}
