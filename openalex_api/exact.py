"""Resumable exact set differences. Key ranges partition work; equality stays exact."""

import hashlib
import json
import os
import sqlite3
from .backend import database, sql_string
from .config import ENTITIES, atomic_json, generation_path
from . import resources


def compare(g, b, left, right, fields, key, entity=None):
    dbname = database(g)
    where = " WHERE entity=" + sql_string(entity) if entity else ""
    identities = b.ch(
        f"SELECT name,uuid FROM system.tables WHERE database={sql_string(dbname)} AND name IN ({sql_string(left)},{sql_string(right)}) ORDER BY name FORMAT JSONEachRow"
    )
    parts = b.ch(
        f"SELECT table,name,rows,data_version FROM system.parts WHERE active AND database={sql_string(dbname)} AND table IN ({sql_string(left)},{sql_string(right)}) ORDER BY table,name FORMAT JSONEachRow"
    )
    # Partitioned sources are scoped to the entity being checked. A merge of
    # unrelated keywords must not invalidate a completed works proof.
    if entity:
        partitioned = [r["name"] for r in b.ch(
            f"SELECT name FROM system.tables WHERE database={sql_string(dbname)} "
            f"AND name IN ({sql_string(left)},{sql_string(right)}) AND partition_key='entity' FORMAT JSONEachRow"
        )]
        if partitioned:
            names = ",".join(sql_string(name) for name in partitioned)
            parts = b.ch(
                f"SELECT table,name,rows,data_version FROM system.parts WHERE active AND database={sql_string(dbname)} "
                f"AND table IN ({sql_string(left)},{sql_string(right)}) "
                f"AND (table NOT IN ({names}) OR partition={sql_string(entity)}) ORDER BY table,name FORMAT JSONEachRow"
            )
    from .ingest import connect
    from .proofs import semantic

    with connect(g) as ledger:
        data_state = [
            tuple(row)
            for row in ledger.execute(
                "SELECT relative,metadata_line,metadata_state FROM shards WHERE selected=1 ORDER BY relative"
            )
        ]
    token = hashlib.sha256(
        json.dumps(
            [identities, parts, fields, key, entity, data_state, semantic(g)],
            sort_keys=True,
        ).encode()
    ).hexdigest()[:24]
    from .exact_fast import compare_ranges

    return compare_ranges(g, b, token, identities, parts, left, right, fields, key, entity)


def audit(g, b, edge, ids):
    errors = []
    # Conflicting digests are an exact GROUP BY predicate, never a checksum approximation.
    from .exact_fast import conflict_count

    n = conflict_count(g, b)
    if int(n):
        errors.append("conflicting source records")
    for entity in ENTITIES:
        if compare(
            g, b, "entities", "audit", "entity,id,digest,source,line", "id", entity
        ):
            errors.append("entity without matching source audit: " + entity)
        if compare(g, b, "audit", "entities", "entity,id", "id", entity):
            errors.append("source audit without entity: " + entity)
        for a, z in ((ids, "identifiers"), ("identifiers", ids)):
            n = compare(g, b, a, z, "entity,scheme,value,id", "id", entity)
            if n:
                errors.append(f"{a} -> {z}: {n} unmatched rows")
    for table in ("references_out", "references_in"):
        for a, z in ((edge, table), (table, edge)):
            n = compare(g, b, a, z, "citing,cited", "citing")
            if n:
                errors.append(f"{a} -> {z}: {n} unmatched rows")
    return errors
