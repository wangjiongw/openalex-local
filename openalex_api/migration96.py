
from .config import CODE_ROOT
"""Explicit v2 cursor lineage. Original cursors, tables and evidence remain intact."""

import hashlib
import json
import sqlite3
import time
from .config import ROOT, atomic_json, generation_path, read_generation
from .backend import database, sql_string
from . import ingest


def shard_identity(shards):
    return hashlib.sha256(
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
    ).hexdigest()


def old_runhash(shards, signature):
    return hashlib.sha256(
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
        + signature.encode()
    ).hexdigest()


def migrate(g, b, legacy_signature, code_hashes):
    from .proofs import semantic

    p = generation_path(g)
    journal = p / "throughput96-migration.json"
    with ingest.lock(p / "import.lock"):
        if journal.exists():
            saved = json.loads(journal.read_text())
            if saved["state"] == "complete":
                return saved
        else:
            saved = dict(
                state="prepared",
                legacy_signature=legacy_signature,
                code_hashes=code_hashes,
                started=time.time(),
            )
            atomic_json(journal, saved)
        backup = p / "before-throughput96"
        backup.mkdir(exist_ok=True)
        with ingest.connect(g) as db:
            target = backup / "tasks.sqlite"
            if not target.exists():
                with sqlite3.connect(target) as out:
                    db.backup(out)
            shards = [
                dict(r) for r in db.execute("SELECT * FROM shards WHERE selected=1")
            ]
        cursor_path = p / "metadata-audit-cursor.json"
        cursor = json.loads(cursor_path.read_text()) if cursor_path.exists() else None
        identities = b.ch(
            f"SELECT name,uuid FROM system.tables WHERE database={sql_string(database(g))} ORDER BY name FORMAT JSONEachRow"
        )
        if cursor:
            expected = old_runhash(shards, saved["legacy_signature"])
            if cursor["runhash"] != expected:
                raise RuntimeError(
                    "legacy cursor signature/data mismatch; migration refused"
                )
            required = {
                "entities",
                "audit",
                "recovery_edges_" + expected[:12],
                "recovery_ids_" + expected[:12],
            }
            if not required <= {r["name"] for r in identities}:
                raise RuntimeError("legacy audit tables missing")
            atomic_json(backup / "metadata-audit-cursor.json", cursor)
            atomic_json(
                p / "audit-lineage.json",
                dict(
                    semantic=semantic(g),
                    shards=shard_identity(shards),
                    runhash=expected,
                    tables=[r for r in identities if r["name"] in required],
                ),
            )
        for name in ("generation.json", "retired-search.json", "execution-plan.json"):
            source = p / name
            if source.exists() and not (backup / name).exists():
                atomic_json(backup / name, json.loads(source.read_text()))
        atomic_json(backup / "table-identities.json", identities)
        # No blanket re-signing. Storage and validation proofs must pass their own dependencies.
        saved.update(
            state="complete",
            semantic=semantic(g),
            shards=shard_identity(shards),
            tables=identities,
            completed=time.time(),
        )
        atomic_json(journal, saved)
        return saved


def inherited_runhash(g, shards, b):
    from .proofs import semantic

    p = generation_path(g) / "audit-lineage.json"
    if not p.exists():
        return None
    saved = json.loads(p.read_text())
    if saved["semantic"] != semantic(g) or saved["shards"] != shard_identity(shards):
        return None
    actual = b.ch(
        f"SELECT name,uuid FROM system.tables WHERE database={sql_string(database(g))} ORDER BY name FORMAT JSONEachRow"
    )
    for row in saved["tables"]:
        if row not in actual:
            raise RuntimeError("inherited audit table identity changed")
    return saved["runhash"]


def legacy_signature(g):
    """Recompute the original v2 fingerprint from the preserved baseline, never guess it."""
    from .config import config

    baseline = CODE_ROOT / "baseline/openalex_api"
    if not baseline.is_dir():
        raise RuntimeError("preserved v2 code is required for migration")
    h = hashlib.sha256()
    for p in sorted(baseline.glob("*.py")):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    for name in (
        "scripts/ReadOnlyIndexCheck.java",
        "scripts/CaptureAgent.java",
        "scripts/benchmark.py",
    ):
        h.update((ROOT / name).read_bytes())
    h.update((ROOT / "config/engines.lock.json").read_bytes())
    h.update(read_generation(g)["manifest_sha256"].encode())
    h.update(
        json.dumps(
            {
                k: v
                for k, v in config().items()
                if not any(secret in k for secret in ("password", "secret", "api_keys"))
            },
            sort_keys=True,
        ).encode()
    )
    return h.hexdigest()
