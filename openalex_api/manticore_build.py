"""Receipt-driven Earth indexes. Only sealed, verified snapshots can be published."""
import argparse
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

import orjson

from .backend import Backend, sql_string
from .config import ROOT, atomic_json, generation_path, read_generation, config
from .earth_control import read_json
from .ingest import lock
from .manticore_engine import Client, create, project, checked_name, ENGINE_VERSION
from .search_versions import build_path, require_metadata

GROUPS = ("01_core_journals", "02_broad_interdisciplinary", "03_author_supported")
MAX_BLOCK = 5_000_000
BATCH = 1000


def file_hash(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024**2), b""):
            value.update(chunk)
    return value.hexdigest()


def signature(layout="legacy"):
    if layout not in ("legacy", "native_v2"):
        raise ValueError("unknown Manticore layout")
    files = ["manticore_engine.py", "manticore_query.py"]
    if layout == "native_v2":
        files.append("manticore_formats.py")
    return hashlib.sha256(orjson.dumps({n: file_hash(ROOT / "openalex_api" / n) for n in files},
                                     option=orjson.OPT_SORT_KEYS)).hexdigest()


def save(m):
    m["updated"] = time.time()
    atomic_json(build_path(m["generation"], m["build"]) / "build.json", m)


def tables(c):
    return {r["Table"] for r in c.rows("SHOW TABLES")}


def verify_rows(c, table, expected):
    if not expected:
        return
    ids = ",".join(str(r["id"]) for r in expected)
    rows = c.rows(f"SELECT id,original_id,source_version,source_hash,projection_hash FROM {checked_name(table)} "
                  f"WHERE id IN ({ids}) LIMIT {len(expected)} OPTION max_matches={len(expected)}")
    actual = {str(r["id"]): r for r in rows}
    if len(actual) != len(expected):
        raise RuntimeError("index receipt cardinality mismatch: " + table)
    for row in expected:
        found = actual.get(str(row["id"]))
        if not found or any(str(found[k]) != str(v) for k, v in row.items()):
            raise RuntimeError("index ID/version/hash mismatch: " + row["original_id"])


def verify_segment(c, path, segment, table=None):
    table = table or segment["index"]
    count = int(c.rows(f"SELECT count(*) AS n FROM {checked_name(table)} OPTION cutoff=0")[0]["n"])
    if count != segment["count"]:
        raise RuntimeError("index has missing or extra records: " + table)
    seen = set()
    for batch in segment["batches"]:
        receipt = path / batch["path"]
        if file_hash(receipt) != batch["sha256"]:
            raise RuntimeError("batch receipt checksum mismatch")
        expected = orjson.loads(gzip.decompress(receipt.read_bytes()))
        keys = {r["id"] for r in expected}
        if len(keys) != len(expected) or keys & seen:
            raise RuntimeError("duplicate index member")
        seen.update(keys)
        verify_rows(c, table, expected)
    if len(seen) != count:
        raise RuntimeError("receipt count mismatch")


def restore(c, path, segment, target=None):
    snap = segment["snapshot"]
    directory = path / snap["path"]
    for f in snap["files"]:
        if file_hash(directory / f["name"]) != f["sha256"]:
            raise RuntimeError("snapshot checksum mismatch")
    target = checked_name(target or segment["index"])
    if target not in tables(c):
        c.sql(f"IMPORT TABLE {target} FROM {sql_string(str(directory / segment['index']))}")
    verify_segment(c, path, segment, target)
    if segment.get("physical_layout") == "native_v2":
        from .manticore_formats import check_indexes
        check_indexes(c, target)


def seal(c, path, segment, protect=None):
    with lock(ROOT / "var/run/manticore-seal.lock"):
        return _seal(c, path, segment, protect)


def _seal(c, path, segment, protect=None):
    if protect:
        protect()
    name = checked_name(segment["index"])
    verify_segment(c, path, segment)
    c.sql(f"FLUSH RAMCHUNK {name}")
    c.sql(f"OPTIMIZE TABLE {name} OPTION sync=1,cutoff=8")
    if segment.get("physical_layout") == "native_v2":
        from .manticore_formats import check_indexes
        segment["index_check"] = check_indexes(c, name)
    files = c.rows(f"FREEZE {name}")
    directory = path / "snapshots" / name
    directory.mkdir(parents=True, exist_ok=True)
    copied = []
    try:
        for f in files:
            if protect:
                protect()
            source = Path(f["file"])
            target = directory / source.name
            temporary = target.with_name(target.name + ".tmp")
            with source.open("rb") as src, temporary.open("wb") as dst:
                shutil.copyfileobj(src, dst, 8 * 1024**2)
                dst.flush()
                os.fsync(dst.fileno())
            expected = file_hash(source)
            if file_hash(temporary) != expected:
                raise RuntimeError("snapshot copy checksum mismatch")
            os.replace(temporary, target)
            copied.append(dict(name=target.name, sha256=expected, bytes=target.stat().st_size))
        segment["snapshot"] = dict(path=str(directory.relative_to(path)), files=copied)
    finally:
        c.sql(f"UNFREEZE {name}")
    physical_check(path, segment)
    if protect:
        protect()
    restored = name + "_restore"
    # A second table proves the persisted copy opens and reconciles independently.
    restore(c, path, segment, restored)
    c.sql(f"DROP TABLE {restored}")
    segment.update(sealed=True, restore_verified=True, sealed_at=time.time())


def physical_check(path, segment):
    from .manticore_engine import runtime_root
    from .manticore_formats import engine_env
    directory = path / segment["snapshot"]["path"]
    binary = ROOT / "var/engines/manticore-29.9.0/usr/bin/indextool"
    output = directory / "indextool.log"
    runtime_root().mkdir(parents=True, exist_ok=True)
    # Check the exact persisted bytes locally; native index checks perform many small reads.
    with tempfile.TemporaryDirectory(prefix="physical-check-", dir=runtime_root()) as temporary:
        local = Path(temporary)
        for f in segment["snapshot"]["files"]:
            target = local / f["name"]
            shutil.copyfile(directory / f["name"], target)
            if file_hash(target) != f["sha256"]:
                raise RuntimeError("snapshot checksum mismatch during physical verification")
        conf = local / "indextool.conf"
        conf.write_text(f"table checked {{\n type = rt\n path = {local / segment['index']}\n rt_field = name\n}}\n")
        with output.open("w") as stream:
            result = subprocess.run([str(binary), "--config", str(conf), "--check", "checked"],
                                    stdout=stream, stderr=subprocess.STDOUT, timeout=3600, env=engine_env())
    if result.returncode or "check passed" not in output.read_text():
        raise RuntimeError("native snapshot consistency check failed: " + str(output))
    segment["physical_verified"] = True


def member_files(collection, manifest, group):
    for n in range(manifest["blocks"]):
        receipt = read_json(collection / "blocks" / f"{n:08d}.json")
        if receipt["sequence"] != n:
            raise RuntimeError("export receipt sequence mismatch")
        for f in receipt["files"]:
            if f["group"] == group and f["path"].endswith(".jsonl.gz"):
                yield f


def members(collection, manifest, group, skip=0):
    for f in member_files(collection, manifest, group):
        if skip >= f["count"]:
            skip -= f["count"]
            continue
        path = collection / f["path"]
        if not path.resolve().is_relative_to(collection.resolve()) or file_hash(path) != f["sha256"]:
            raise RuntimeError("export file path/checksum mismatch")
        count = 0
        with gzip.open(path, "rb") as stream:
            for line in stream:
                count += 1
                row = orjson.loads(line)
                if skip:
                    skip -= 1
                    continue
                if row["group"] != group:
                    raise RuntimeError("export group mismatch")
                row["id"] = row["openalex_id"].rsplit("/", 1)[-1]
                yield row
        if count != f["count"]:
            raise RuntimeError("export receipt count mismatch")


def prepare_document(args):
    entity, group, row, member = args[:4]
    layout = args[4] if len(args) > 4 else "legacy"
    if member and (str(row["version"]) != str(member["source_version"]) or row["digest"] != member["raw_digest"]):
        raise RuntimeError("frozen export/metadata version or hash mismatch: " + row["id"])
    from .manticore_formats import project_native
    projector = project_native if layout == "native_v2" else project
    numeric, doc = projector(entity, row, group, member.get("reasons", []))
    if doc["source_hash"] != row["digest"]:
        raise RuntimeError("metadata raw hash mismatch")
    return numeric, doc


def write_batch(c, b, path, segment, rows, membership=None, pool=None, bulk_workers=1):
    if bulk_workers not in (1, 2, 4):
        raise ValueError("unsupported bulk concurrency")
    started = time.monotonic()
    membership = {r["id"]: r for r in membership} if membership is not None else None
    if membership is not None and len(rows) != len(membership):
        raise RuntimeError("missing or duplicate metadata member")
    inputs = [(segment["entity"], segment.get("group", ""), row,
               membership[row["id"]] if membership is not None else {}, segment.get("physical_layout", "legacy")) for row in rows]
    documents = list(pool.map(prepare_document, inputs, chunksize=100)) if pool else list(map(prepare_document, inputs))
    projected = time.monotonic()
    packets, packet, size = [], [], 0
    packet_rows = max(1, (len(documents) + bulk_workers - 1) // bulk_workers)
    for key, doc in documents:
        length = len(orjson.dumps({"replace": {"index": segment["index"], "id": key, "doc": doc}})) + 1
        if length > 32 * 1024**2:
            raise RuntimeError("single search document exceeds 32 MiB packet budget")
        if packet and (size + length > 32 * 1024**2 or len(packet) >= packet_rows):
            packets.append(packet)
            packet, size = [], 0
        packet.append((key, doc))
        size += length
    if packet:
        packets.append(packet)
    if bulk_workers == 1:
        for packet in packets:
            c.bulk(segment["index"], packet)
    else:
        # Commit the receipt only after every packet has acknowledged; replay stays idempotent.
        with ThreadPoolExecutor(bulk_workers) as writers:
            list(writers.map(lambda packet: c.bulk(segment["index"], packet), packets))
    written = time.monotonic()
    expected = [dict(id=k, **{f: d[f] for f in ("original_id", "source_version", "source_hash", "projection_hash")})
                for k, d in documents]
    verify_rows(c, segment["index"], expected)
    verified = time.monotonic()
    target = path / "receipts" / segment["index"] / f"{len(segment['batches']):07d}.json.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    with temporary.open("wb") as out:
        out.write(gzip.compress(orjson.dumps(expected), mtime=0))
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, target)
    directory_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    segment["batches"].append(dict(path=str(target.relative_to(path)), sha256=file_hash(target), count=len(rows)))
    segment["count"] += len(rows)
    segment["after"] = rows[-1]["id"]
    segment["invalid_abstracts"] = segment.get("invalid_abstracts", 0) + sum(d["attrs"]["invalid_abstract"] for _, d in documents)
    return dict(projection_seconds=projected-started, bulk_seconds=written-projected,
                verify_seconds=verified-written, receipt_seconds=time.monotonic()-verified)


def build(g, build_id, groups, parent=None, layout=None, source=None):
    from .official import source_table
    from .manticore_pipeline import artifact_signature, parent_ready, guard
    from .search_versions import read
    if source and parent:
        raise ValueError("source rebuild and incremental parent are mutually exclusive")
    if parent:
        inherited_layout = read(g, parent).get("physical_layout", "legacy")
        if layout and layout != inherited_layout:
            raise ValueError("incremental child must inherit parent layout")
        layout = inherited_layout
    layout = layout or "legacy"
    projection_signature = signature(layout)
    from .manticore_formats import create as create_native
    creator = create_native if layout == "native_v2" else create
    original = read(g, source) if source else None
    path = build_path(g, build_id)
    collection = ROOT / "exports/earth_collections" / g / "earth_scope_v1"
    frozen = read_json(collection / "manifest.json")
    verified = read_json(collection / "verification.json")
    if not verified.get("passed") or not verified.get("complete") or verified["signature"] != frozen["signature"]:
        raise RuntimeError("complete verified frozen Earth collection required")
    proof = require_metadata(g)
    if original and (original["state"] not in ("sealed", "validated")
            or original["collection_signature"] != frozen["signature"]
            or original["metadata_signature"] != proof["signature"]
            or original["groups"] != groups):
        raise RuntimeError("rebuild source is not the same sealed collection/metadata/groups")
    try:
        m = read_json(path / "build.json")
    except FileNotFoundError:
        m = None
    if m is not None:
        if (m["projection_signature"] != projection_signature or m["metadata_signature"] != proof["signature"]
                or m["collection_signature"] != frozen["signature"] or m.get("input_groups", m["groups"]) != groups
                or m.get("parent") != parent or m.get("rebuild_source") != source
                or (original and m.get("source_artifact_signature") != artifact_signature(original))):
            raise RuntimeError("build inputs changed; create a new build ID")
        if m["state"] in ("sealed", "validated"):
            return m
    else:
        m = dict(generation=g, build=build_id, engine="manticore", engine_version=ENGINE_VERSION,
                 projection_signature=projection_signature, physical_layout=layout, metadata_signature=proof["signature"],
                 rebuild_source=source, source_artifact_signature=artifact_signature(original) if original else None,
                 collection="earth_scope_v1", collection_signature=frozen["signature"],
                 groups=list(groups), input_groups=list(groups), parent=parent,
                 expected_groups={k: verified["counts"][k] for k in groups}, work_limit=20_000_000,
                 earth_expected=sum(verified["counts"].values()), state="building", created=time.time(),
                 incremental=True, global_complete=False, ready_scopes=[], indexes={}, entities={}, segments=[])
        if original:
            m["work_limit"] = original["entities"]["works"]["count"]
        if parent:
            from .search_versions import read
            previous = read(g, parent)
            if (previous.get("engine") != "manticore" or not parent_ready(g, parent)
                    or previous["collection_signature"] != frozen["signature"]
                    or previous["metadata_signature"] != proof["signature"]):
                raise RuntimeError("parent must pass preflight and be observing or validated")
            m["parent_artifact_signature"] = artifact_signature(previous)
            parent_path = build_path(g, parent)
            m["groups"] = list(dict.fromkeys(previous["groups"] + groups))
            m["entities"] = previous["entities"]
            m["segments"] = previous["segments"]
            for segment in m["segments"]:
                for batch in segment["batches"]:
                    batch["path"] = str(parent_path / batch["path"])
                segment["snapshot"]["path"] = str(parent_path / segment["snapshot"]["path"])
            if previous["entities"]["works"]["count"] >= 20_000_000:
                observation = read_json(parent_path / "observation.json")
                acceptance = read_json(parent_path / "qualification/acceptance.json")
                if observation.get("state") not in ("observing", "complete") or not acceptance.get("performance_passed"):
                    raise RuntimeError("20M parent must pass stability and load qualification before expansion")
                m["work_limit"] = m["earth_expected"]
        save(m)
    atomic_json(generation_path(g) / "manticore-candidate.json", dict(build=build_id))
    c, b = Client(timeout=7200), Backend(writer=True)
    tuning = config().get("manticore_build", {})
    tuning_path = generation_path(g) / "manticore-tuning.json"
    if not tuning and tuning_path.exists():
        measured = read_json(tuning_path)
        if measured.get("passed") and measured.get("projection_signature") == projection_signature and measured.get("sustained_seconds", 0) >= 1800:
            tuning = measured["selected"]
    if layout == "native_v2" and not tuning:
        # Authorized initial parameters; native throughput is measured by this actual build.
        tuning = dict(batch_rows=2000, projection_workers=4)
    batch_rows = int(tuning.get("batch_rows", BATCH))
    workers = int(tuning.get("projection_workers", 1))
    bulk_workers = int(tuning.get("bulk_workers", 1))
    if (batch_rows not in (BATCH, 1000, 2000, 5000) or workers not in (1, 2, 4, 8, 16)
            or bulk_workers not in (1, 2, 4)):
        raise ValueError("unsupported Manticore build tuning")
    m["build_tuning"] = dict(batch_rows=batch_rows, projection_workers=workers,
                             bulk_workers=bulk_workers)
    pool = ProcessPoolExecutor(workers, mp_context=multiprocessing.get_context("spawn")) if workers > 1 else None
    prefetch = ThreadPoolExecutor(1)
    reader = Backend(writer=True)
    def fetch(batch, source):
        ids = ",".join(sql_string(r["id"]) for r in batch)
        return reader.ch(f"SELECT id,raw,version,digest FROM {source} WHERE entity='works' AND id IN ({ids}) ORDER BY id FORMAT JSONEachRow")
    saved_at, saved_batches = time.monotonic(), 0
    def finish(segment):
        save(m)
        atomic_json(path / "progress.json", dict(state="sealing", updated=time.time(),
                    entities=m["entities"], segment=segment["index"],
                    phase="compact_snapshot_physical_check_restore"))
        seal(c, path, segment, protect=(lambda: guard(g, m, force=True)) if parent or source else None)
        save(m)
    try:
        m.update(state="building", error=None)
        if parent and not m.get("parent_artifact_signature"):
            m["parent_artifact_signature"] = artifact_signature(read_json(build_path(g, parent) / "build.json"))
        save(m)
        atomic_json(path / "progress.json", dict(state="restoring", updated=time.time(), entities=m["entities"]))
        for segment in m["segments"]:
            if segment.get("sealed"):
                restore(c, path, segment)
            elif segment["index"] not in tables(c):
                creator(c, segment["index"])
                segment.update(count=0, batches=[], after="")
        meta = read_generation(g)
        for entity, group in [("works", k) for k in m["groups"]] + [("topics", ""), ("sources", "")]:
            source = source_table(b, meta, entity)
            selected = [s for s in m["segments"] if s["entity"] == entity and s.get("group", "") == group]
            done = sum(s["count"] for s in selected)
            expected = verified["counts"][group] if group else int(meta["expected"][entity]["records"])
            segment = selected[-1] if selected and not selected[-1].get("sealed") else None
            if segment and (segment["count"] == MAX_BLOCK or done == expected):
                finish(segment)
                save(m)
                segment = None
            iterator = iter(members(collection, frozen, group, done)) if group else None
            pending = None
            while done < expected:
                if m.get("parent") or source:
                    guard(g, m)
                total_works = sum(s["count"] for s in m["segments"] if s["entity"] == "works")
                if entity == "works" and total_works >= m.get("work_limit", 20_000_000):
                    if segment is not None:
                        finish(segment)
                        save(m)
                    m["scale_gate"] = True
                    break
                if segment is None:
                    name = "oa_mc_" + hashlib.sha256((g + build_id).encode()).hexdigest()[:12] + f"_{len(m['segments']):03d}"
                    if name in tables(c):
                        raise RuntimeError("unreceipted table exists; investigate before rebuilding " + name)
                    creator(c, name)
                    segment = dict(index=name, entity=entity, group=group, owner="earth_sciences" if group else entity,
                                   count=0, batches=[], after="", sealed=False, physical_layout=layout)
                    m["segments"].append(segment)
                    save(m)
                size = min(batch_rows, expected - done, MAX_BLOCK - segment["count"])
                if group:
                    size = min(size, m.get("work_limit", 20_000_000) - total_works)
                read_started = time.monotonic()
                if group:
                    if pending is None:
                        batch = [next(iterator) for _ in range(size)]
                        rows = fetch(batch, source)
                    else:
                        batch, future = pending
                        rows = future.result()
                        pending = None
                    next_size = min(batch_rows, expected-done-size, MAX_BLOCK-segment["count"]-size,
                                    m.get("work_limit", 20_000_000)-total_works-size)
                    if next_size > 0:
                        following = [next(iterator) for _ in range(next_size)]
                        pending = (following, prefetch.submit(fetch, following, source))
                else:
                    batch = None
                    after = segment["after"] or (selected[-1]["after"] if selected else "")
                    rows = b.ch(f"SELECT id,raw,version,digest FROM {source} WHERE entity={sql_string(entity)} "
                                f"AND id>{sql_string(after)} ORDER BY id LIMIT {size} FORMAT JSONEachRow")
                if len(rows) != size:
                    raise RuntimeError("incomplete metadata batch")
                read_wait = time.monotonic() - read_started
                timings = write_batch(c, b, path, segment, rows, batch, pool, bulk_workers)
                timings["source_wait_seconds"] = read_wait
                done += len(rows)
                m["entities"][entity] = dict(count=sum(s["count"] for s in m["segments"] if s["entity"] == entity))
                saved_batches += 1
                if saved_batches >= 10 or time.monotonic()-saved_at >= 30:
                    save(m)
                    saved_at, saved_batches = time.monotonic(), 0
                atomic_json(path / "progress.json", dict(state="building", updated=time.time(), entities=m["entities"],
                            checkpoint_updated=m["updated"], **m["build_tuning"], batch_timings=timings))
                print(json.dumps(dict(entity=entity, group=group, count=done, expected=expected,
                                      batch_rows=len(rows), timings=timings)), flush=True)
                if segment["count"] == MAX_BLOCK or done == expected:
                    save(m)
                    if parent:
                        guard(g, m, force=True)
                    finish(segment)
                    save(m)
                    selected.append(segment)
                    segment = None
        m["indexes"] = {e: ",".join(s["index"] for s in m["segments"] if s["entity"] == e) for e in m["entities"]}
        m["expected_groups"] = {k: sum(s["count"] for s in m["segments"] if s.get("group") == k) for k in m["groups"]}
        if original:
            from .manticore_membership import reconcile
            m["source_reconciliation"] = reconcile(g, original, m)
        m.update(state="sealed", sealed_at=time.time(), ready_scopes=["earth_sciences"],
                 earth_complete=m["entities"]["works"]["count"] == m["earth_expected"])
        save(m)
        atomic_json(path / "progress.json", dict(state="sealed", updated=time.time(), entities=m["entities"]))
        return m
    except Exception as error:
        m.update(state="failed", error=str(error))
        save(m)
        raise
    finally:
        prefetch.shutdown(wait=True, cancel_futures=True)
        reader.close()
        if pool:
            pool.shutdown(wait=True, cancel_futures=True)
        c.close()
        b.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("build", "restore", "status", "qualify", "publish", "mixed"))
    parser.add_argument("generation")
    parser.add_argument("build_id")
    parser.add_argument("--groups", default=GROUPS[0])
    parser.add_argument("--parent")
    parser.add_argument("--layout", choices=("legacy", "native_v2"))
    parser.add_argument("--source")
    parser.add_argument("--child")
    parser.add_argument("--background", action="store_true")
    parser.add_argument("--qualify-after", action="store_true")
    parser.add_argument("--wait-build", action="store_true")
    parser.add_argument("--publish-after", action="store_true")
    parser.add_argument("--continue-earth", action="store_true")
    args = parser.parse_args()
    path = build_path(args.generation, args.build_id)
    groups = args.groups.split(",")
    if not groups or len(set(groups)) != len(groups) or set(groups) - set(GROUPS):
        parser.error("invalid groups")
    if args.background:
        from .manage import start
        extra = [args.action, args.generation, args.build_id, "--groups", args.groups]
        for name in ("layout", "source", "child"):
            if getattr(args, name):
                extra += ["--" + name, getattr(args, name)]
        if args.parent:
            extra += ["--parent", args.parent]
        if args.qualify_after:
            extra.append("--qualify-after")
        if args.wait_build:
            extra.append("--wait-build")
        if args.publish_after:
            extra.append("--publish-after")
        if args.continue_earth:
            extra.append("--continue-earth")
        print(start(("build-" if args.action == "build" else "verify-") + "manticore-" + args.build_id, extra))
        return
    if args.action == "mixed":
        if not args.child:
            parser.error("mixed requires --child")
        from .manticore_pipeline import mixed_check
        mixed_check(args.generation, args.build_id, args.child)
        return
    if args.action == "status":
        m = read_json(path / "build.json")
        print(json.dumps({k: v for k, v in m.items() if k != "segments"}, indent=2))
        return
    if args.wait_build:
        from .manage import identity, state_path
        while read_json(path / "build.json")["state"] == "building":
            service = state_path("build-manticore-" + args.build_id)
            if not identity(service) and not identity(service, child=True):
                raise RuntimeError("build supervisor is no longer running")
            time.sleep(30)
    with lock(path / "control.lock"):
        if args.action == "build":
            build(args.generation, args.build_id, groups, args.parent, args.layout, args.source)
        if args.action == "restore":
            c = Client(timeout=300)
            try:
                for segment in read_json(path / "build.json")["segments"]:
                    restore(c, path, segment)
            finally:
                c.close()
        if args.action == "qualify" or args.action == "build" and args.qualify_after:
            from .manticore_qualification import qualify
            try:
                qualify(args.generation, args.build_id)
            except Exception as error:
                try:
                    observation = read_json(path / "observation.json")
                except FileNotFoundError:
                    observation = {"build": args.build_id}
                observation.update(state="failed", error=str(error), updated=time.time())
                atomic_json(path / "observation.json", observation)
                raise
        if args.action == "publish" or args.publish_after:
            from .manticore_qualification import publish
            publish(args.generation, args.build_id)
            if args.continue_earth:
                from .manticore_pipeline import continue_earth
                m = read_json(path / "build.json")
                continue_earth(args.generation, m)


if __name__ == "__main__":
    main()
