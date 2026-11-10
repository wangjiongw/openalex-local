"""Exact reconciliation of rebuild members against immutable source receipts."""
import gzip
import sqlite3
import tempfile
import time
from pathlib import Path

import orjson

from .config import atomic_json
from .manticore_build import file_hash
from .manticore_engine import runtime_root
from .search_versions import build_path


def reconcile(g, original, candidate):
    if candidate["entities"] != original["entities"]:
        raise RuntimeError("rebuild entity counts differ from source")
    path = build_path(g, candidate["build"])
    atomic_json(path / "progress.json", dict(state="reconciling_source", updated=time.time(),
                                            entities=candidate["entities"]))
    def rows(manifest):
        root = build_path(g, manifest["build"])
        for segment in manifest["segments"]:
            for batch in segment["batches"]:
                receipt = root / batch["path"]
                if file_hash(receipt) != batch["sha256"]:
                    raise RuntimeError("rebuild receipt checksum mismatch")
                yield [(segment["entity"], r["original_id"], str(r["source_version"]), r["source_hash"])
                       for r in orjson.loads(gzip.decompress(receipt.read_bytes()))]
    with tempfile.TemporaryDirectory(prefix="member-check-", dir=runtime_root()) as directory:
        with sqlite3.connect(Path(directory) / "members.sqlite") as db:
            db.execute("CREATE TABLE members(entity TEXT,id TEXT,version TEXT,hash TEXT,PRIMARY KEY(entity,id)) WITHOUT ROWID")
            total = 0
            for batch in rows(original):
                db.executemany("INSERT INTO members VALUES(?,?,?,?)", batch)
                total += len(batch)
            db.commit()
            for batch in rows(candidate):
                before = db.total_changes
                db.executemany("DELETE FROM members WHERE entity=? AND id=? AND version=? AND hash=?", batch)
                if db.total_changes - before != len(batch):
                    raise RuntimeError("rebuild contains duplicate, changed or additional members")
            if db.execute("SELECT count(*) FROM members").fetchone()[0]:
                raise RuntimeError("rebuild is missing source members")
    return dict(passed=True, source=original["build"], records=total, checked_at=time.time())
