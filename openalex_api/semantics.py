"""Data contract only. Execution tuning, queues and logging must stay outside this module."""

import hashlib
import orjson
from .model import canonical_id, numeric_id, external_ids


def parse(raw):
    record = orjson.loads(raw)
    if not isinstance(record, dict):
        raise ValueError("record must be an object")
    return record


def digest(record):
    return hashlib.sha256(orjson.dumps(record, option=orjson.OPT_SORT_KEYS)).hexdigest()


def entity_row(entity, line, ordinal, source, raw, record):
    key = canonical_id(record.get("id"))
    if line >= 1000000000:
        raise ValueError("shard exceeds version line capacity")
    return dict(
        entity=entity,
        id=key,
        numeric_id=numeric_id(key),
        version=ordinal * 1000000000 + line,
        source=source,
        line=line,
        raw=raw.decode("utf-8").rstrip("\r\n"),
        digest=digest(record),
        cited_by_count=record.get("cited_by_count"),
        publication_year=record.get("publication_year"),
    )


def audit_row(row):
    return {k: row[k] for k in ("entity", "id", "source", "line", "digest", "version")}


def identifiers(entity, key, version, record):
    for scheme, value in external_ids(record):
        yield dict(entity=entity, scheme=scheme, value=value, id=key, version=version)


def references(key, version, record):
    citing = numeric_id(key)
    if not key.startswith("W") or citing is None:
        raise ValueError("work must have W numeric ID")
    for cited in set(record.get("referenced_works") or []):
        target = canonical_id(cited)
        if not target.startswith("W") or numeric_id(target) is None:
            raise ValueError("reference must have W numeric ID")
        yield dict(citing=citing, cited=numeric_id(target), version=version)


def verify(row):
    record = parse(row["raw"])
    if digest(record) != row["digest"] or canonical_id(record.get("id")) != row["id"]:
        raise RuntimeError("Raw digest/ID mismatch: " + row["id"])
    return record
