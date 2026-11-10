"""Stage evidence, ledger migration and resumable metadata audit."""

import hashlib
import json
import logging
import sqlite3
import time
from pathlib import Path
import orjson
from .config import (
    ROOT,
    ENTITIES,
    config,
    generation_path,
    read_generation,
    atomic_json,
)
from .backend import Backend, database, sql_string
from .model import external_ids, canonical_id, numeric_id
from . import ingest

LOG = logging.getLogger(__name__)


def signature(g):
    from .proofs import semantic

    return semantic(g)


def evidence(g, name, **data):
    from .proofs import dependencies

    dependency_set = dependencies(g, name)
    if (
        data.get("passed")
        and "epoch" in data
        and dependency_set.get("search_epoch", data["epoch"]) != data["epoch"]
    ):
        raise RuntimeError("index allocation changed while issuing evidence")
    result = dict(
        generation=g,
        signature=signature(g),
        dependencies=dependency_set,
        time=time.time(),
        **data,
    )
    atomic_json(generation_path(g) / (name + ".json"), result)
    return result


def require_evidence(g, name):
    p = generation_path(g) / (name + ".json")
    if not p.exists():
        raise RuntimeError("Missing evidence: " + name)
    r = json.loads(p.read_text())
    if not r.get("passed") or r.get("signature") != signature(g):
        raise RuntimeError("Failed or stale evidence: " + name)
    from .proofs import dependencies

    current = dependencies(g, name)
    if r.get("dependencies") != current:
        from .proofs import legacy_archive_compatible
        if not legacy_archive_compatible(g, name, r.get("dependencies", {}), current):
            raise RuntimeError("Failed or stale evidence dependencies: " + name)
    return r


def require_stage(g, stage):
    meta = read_generation(g)
    if meta.get("pipeline_version") != 2:
        if (generation_path(g) / "resume-block.json").exists():
            raise RuntimeError("Recovery migration and stage validation required")
        return
    require_evidence(g, "gate-" + stage)


def release(g, stage):
    with ingest.lock(generation_path(g) / "import.lock"):
        require_evidence(g, "recovery-tests")
        require_evidence(g, "archive")
        from .resume import require_search_disposition

        require_search_disposition(g)
        if stage == "metadata":
            require_evidence(g, "metadata-audit")
        elif stage == "search":
            require_evidence(g, "metadata-complete")
            require_evidence(g, "storage-verification")
        else:
            raise ValueError("stage must be metadata or search")
        return evidence(g, "gate-" + stage, passed=True)


def migrate(g):
    p = generation_path(g)
    with ingest.lock(p / "import.lock"):
        meta = read_generation(g)
        if meta.get("validated"):
            raise RuntimeError("cannot migrate an immutable published generation")
        with ingest.connect(g) as src:
            dest = p / "tasks-before-v2.sqlite"
            if not dest.exists():
                with sqlite3.connect(dest) as out:
                    src.backup(out)
                dest.chmod(0o600)
            columns = {r["name"] for r in src.execute("PRAGMA table_info(shards)")}
            for name, definition in [
                ("metadata_line", "INTEGER DEFAULT 0"),
                ("metadata_state", "TEXT DEFAULT 'pending'"),
                ("metadata_prefix_sha256", "TEXT"),
                ("legacy_line", "INTEGER DEFAULT 0"),
                ("legacy_state", "TEXT DEFAULT 'pending'"),
                ("legacy_verified", "INTEGER DEFAULT 0"),
            ]:
                if name not in columns:
                    src.execute(f"ALTER TABLE shards ADD COLUMN {name} {definition}")
            if meta.get("pipeline_version") != 2:
                src.execute(
                    "UPDATE shards SET legacy_line=line,legacy_state=state,metadata_line=line,metadata_prefix_sha256=prefix_sha256,metadata_state=state"
                )
            src.execute(
                "CREATE TABLE IF NOT EXISTS search_tasks(entity TEXT PRIMARY KEY,last_id TEXT DEFAULT '',checked INTEGER DEFAULT 0,repaired INTEGER DEFAULT 0,state TEXT DEFAULT 'pending',error TEXT,updated REAL)"
            )
            src.executemany(
                "INSERT OR IGNORE INTO search_tasks(entity) VALUES(?)",
                [(e,) for e in ENTITIES],
            )
            src.execute(
                "CREATE TABLE IF NOT EXISTS events(time REAL,stage TEXT,kind TEXT,message TEXT)"
            )
        meta.update(pipeline_version=2)
        atomic_json(p / "generation.json", meta)
        return evidence(g, "migration", passed=True, backup=str(dest))


def event(g, stage, kind, message):
    with ingest.connect(g) as db:
        db.execute(
            "INSERT INTO events VALUES(?,?,?,?)",
            (time.time(), stage, kind, str(message)[:5000]),
        )
    LOG.info("%s %s: %s", stage, kind, message)


def _coverage(b, g):
    db = database(g)
    rows = b.ch(
        f"SELECT source,count() n,uniqExact(line) unique_lines,min(line) first,max(line) last FROM {db}.audit FINAL GROUP BY source FORMAT JSONEachRow"
    )
    actual = {r["source"]: r for r in rows}
    errors = []
    with ingest.connect(g) as ledger:
        shards = [
            dict(r) for r in ledger.execute("SELECT * FROM shards WHERE selected=1")
        ]
    for s in shards:
        prefix = s.get("metadata_line", s["line"])
        r = actual.get(
            s["relative"], {"n": 0, "unique_lines": 0, "first": 0, "last": 0}
        )
        if (
            int(r["n"]) != int(r["unique_lines"])
            or (r["n"] and (int(r["first"]) != 1 or int(r["last"]) != int(r["n"])))
            or int(r["n"]) < prefix
            or int(r["n"]) > s["expected"]
        ):
            errors.append(s["relative"] + ": noncontiguous/insufficient source audit")
        if (
            s.get("metadata_state", s["state"]) == "done"
            and int(r["n"]) != s["expected"]
        ):
            errors.append(s["relative"] + ": completed shard count mismatch")
        path = Path(s["path"])
        if not path.is_file() or path.stat().st_size != s["bytes"]:
            errors.append(s["relative"] + ": source file missing or changed")
    return errors, shards


def audit(g, force=False):
    """Only raw/metadata reads; expected relations are disposable, replay-safe audit tables."""
    p = generation_path(g)
    with ingest.lock(p / "import.lock"):
        meta = read_generation(g)
        if (
            ingest.inventory(meta["snapshot"])["manifest_sha256"]
            != meta["manifest_sha256"]
        ):
            raise RuntimeError("snapshot manifest changed")
        b = Backend(writer=True)
        db = database(g)
        try:
            errors, shards = _coverage(b, g)
            if errors:
                return evidence(g, "metadata-audit", passed=False, errors=errors)
            runhash = hashlib.sha256(
                json.dumps(
                    [
                        (
                            s["relative"],
                            s.get("metadata_line", s["line"]),
                            s.get("metadata_state", s["state"]),
                        )
                        for s in shards
                    ],
                    sort_keys=True,
                ).encode()
                + signature(g).encode()
            ).hexdigest()
            if config().get("execution_profile") == "throughput96":
                from .migration96 import inherited_runhash

                runhash = inherited_runhash(g, shards, b) or runhash
            try:
                cached = require_evidence(g, "metadata-audit")
                if not force and cached.get("runhash") == runhash:
                    return cached
            except RuntimeError:
                pass
            edge = "recovery_edges_" + runhash[:12]
            ids = "recovery_ids_" + runhash[:12]
            b.ch(
                f"CREATE TABLE IF NOT EXISTS {db}.{edge} (citing UInt64,cited UInt64,version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY(citing,cited)"
            )
            b.ch(
                f"CREATE TABLE IF NOT EXISTS {db}.{ids} (entity LowCardinality(String),scheme String,value String,id String,version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY(entity,scheme,value,id)"
            )
            cursor_path = p / "metadata-audit-cursor.json"
            state = json.loads(cursor_path.read_text()) if cursor_path.exists() else {}
            if state.get("runhash") != runhash:
                state = {"runhash": runhash, "entities": {}, "checked": 0}
            if config().get("execution_profile") == "throughput96":
                from .intervals import run

                state["checked"] = run(g, b, runhash, state, edge, ids)
            else:
                for entity in ENTITIES:
                    ent = state["entities"].setdefault(
                        entity, {"last_id": "", "done": False}
                    )
                    while not ent["done"]:
                        rows = b.ch(
                            f"SELECT id,raw,digest,version,source,line FROM {db}.entities FINAL WHERE entity={sql_string(entity)} AND id>{sql_string(ent['last_id'])} ORDER BY id LIMIT 1000 FORMAT JSONEachRow"
                        )
                        if not rows:
                            ent["done"] = True
                            atomic_json(cursor_path, state)
                            break
                        refs = []
                        lookups = []
                        for r in rows:
                            record = orjson.loads(r["raw"])
                            digest = hashlib.sha256(
                                orjson.dumps(record, option=orjson.OPT_SORT_KEYS)
                            ).hexdigest()
                            if (
                                digest != r["digest"]
                                or canonical_id(record.get("id")) != r["id"]
                            ):
                                raise RuntimeError("Raw digest/ID mismatch: " + r["id"])
                            lookups.extend(
                                dict(
                                    entity=entity,
                                    scheme=k,
                                    value=v,
                                    id=r["id"],
                                    version=r["version"],
                                )
                                for k, v in external_ids(record)
                            )
                            if entity == "works":
                                refs.extend(
                                    dict(
                                        version=r["version"],
                                        citing=numeric_id(r["id"]),
                                        cited=numeric_id(canonical_id(v)),
                                    )
                                    for v in set(record.get("referenced_works") or [])
                                )
                        b.insert(g, edge, refs)
                        b.insert(g, ids, lookups)
                        ent["last_id"] = rows[-1]["id"]
                        state["checked"] += len(rows)
                        state["updated"] = time.time()
                        atomic_json(cursor_path, state)
            if config().get("execution_profile") == "throughput96":
                from .exact import audit as exact_audit

                errors.extend(exact_audit(g, b, edge, ids))
            else:
                # Source audit matches every retained entity, not just total counts.
                conflicts = b.ch(
                    f"SELECT count() n FROM (SELECT entity,id FROM {db}.audit FINAL GROUP BY entity,id HAVING uniqExact(digest)>1) FORMAT JSONEachRow"
                )[0]["n"]
                if int(conflicts):
                    errors.append("conflicting source records")
                missing = b.ch(
                    f"SELECT count() n FROM (SELECT entity,id,digest,source,line FROM {db}.entities FINAL) l LEFT ANTI JOIN (SELECT entity,id,digest,source,line FROM {db}.audit FINAL) r USING(entity,id,digest,source,line) FORMAT JSONEachRow"
                )[0]["n"]
                if int(missing):
                    errors.append("entity without matching source audit")
                missing = b.ch(
                    f"SELECT count() n FROM (SELECT DISTINCT entity,id FROM {db}.audit FINAL) l LEFT ANTI JOIN (SELECT entity,id FROM {db}.entities FINAL) r USING(entity,id) FORMAT JSONEachRow"
                )[0]["n"]
                if int(missing):
                    errors.append("source audit without entity")
                for left, right, fields in [
                    (edge, "references_out", "citing,cited"),
                    (edge, "references_in", "citing,cited"),
                    (ids, "identifiers", "entity,scheme,value,id"),
                ]:
                    for a, z in [(left, right), (right, left)]:
                        n = b.ch(
                            f"SELECT count() n FROM (SELECT {fields} FROM {db}.{a} FINAL) l LEFT ANTI JOIN (SELECT {fields} FROM {db}.{z} FINAL) r USING({fields}) FORMAT JSONEachRow"
                        )[0]["n"]
                        if int(n):
                            errors.append(f"{a} -> {z}: {n} unmatched rows")
            if not errors:
                with ingest.connect(g) as ledger:
                    ledger.execute(
                        "UPDATE shards SET legacy_verified=1 WHERE selected=1"
                    )
                # These are only regenerated audit tables, never service records.
                if config().get("execution_profile") != "throughput96":
                    b.ch(f"DROP TABLE {db}.{edge} SYNC")
                    b.ch(f"DROP TABLE {db}.{ids} SYNC")
                    cursor_path.unlink(missing_ok=True)
            return evidence(
                g,
                "metadata-audit",
                passed=not errors,
                errors=errors,
                records_checked=state["checked"],
                runhash=runhash,
            )
        finally:
            b.close()


def metadata_complete(g):
    with ingest.connect(g) as db:
        incomplete = db.execute(
            "SELECT count(*) n FROM shards WHERE selected=1 AND metadata_state!='done'"
        ).fetchone()["n"]
    if incomplete:
        raise RuntimeError("metadata shards incomplete")
    from .resume import audit_metadata

    result = audit_metadata(g)
    if not result["passed"]:
        raise RuntimeError(str(result["errors"]))
    meta = read_generation(g)
    meta["metadata_frozen"] = True
    atomic_json(generation_path(g) / "generation.json", meta)
    return evidence(g, "metadata-complete", passed=True, audit=result)
