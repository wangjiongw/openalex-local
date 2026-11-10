"""Independent search builds over an immutable, already audited metadata generation."""
import contextlib
import contextvars
import json
import math
import re
import time
from functools import lru_cache
from pathlib import Path

import orjson
from .config import ROOT, ENTITIES, config, generation_path, atomic_json
from . import search_projection

CONTEXT = contextvars.ContextVar("openalex_search_build", default=None)


def build_path(g, build):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", build):
        raise ValueError("invalid search build identifier")
    return generation_path(g) / "searches" / build


def signature():
    from .proofs import file_hash, digest
    return digest({name: file_hash(ROOT / "openalex_api" / name)
                   for name in ("model.py", "search_projection.py", "search_scopes.py")})


def read(g, build):
    from .earth_control import read_json
    manifest = read_json(build_path(g, build) / "build.json")
    if manifest["generation"] != g or manifest["build"] != build:
        raise RuntimeError("search build identity mismatch")
    if manifest.get("engine") == "manticore":
        from .manticore_build import signature as expected_signature
        expected = expected_signature(manifest.get("physical_layout", "legacy"))
    else:
        expected = signature()
    if manifest["projection_signature"] != expected:
        raise RuntimeError("search projection changed; rebuild required")
    return manifest


def active(g):
    pointer = generation_path(g) / "search-active.json"
    if not pointer.exists():
        return None
    value = json.loads(pointer.read_text())
    if value.get("build") is None:
        return None
    manifest = read(g, value["build"])
    if manifest.get("state") != "validated":
        raise RuntimeError("active search build has not passed validation")
    return manifest


@lru_cache(maxsize=8)
def _request_manifest(g, build, file_identity, projection_signature):
    manifest = read(g, build)
    if manifest["state"] not in ("sealed", "validated"):
        raise RuntimeError("search build is not sealed")
    return manifest


def request_manifest(g, build=None):
    if build is None:
        pointer = generation_path(g) / "search-active.json"
        if not pointer.exists():
            return None
        build = json.loads(pointer.read_text()).get("build")
        if not build:
            return None
    from .manticore_build import signature as manticore_signature
    p = build_path(g, build) / "build.json"
    stat = p.stat()
    # Only request readers share this object; builders continue using uncached read().
    return _request_manifest(g, build, (str(p), stat.st_ino, stat.st_mtime_ns, stat.st_size),
                             (manticore_signature(), manticore_signature("native_v2"), signature()))


def attach(meta, build=None, allow_sealed=False):
    """Pin one manifest for a request; metadata remains usable if search is invalid."""
    meta = dict(meta)
    try:
        manifest = request_manifest(meta["generation"], build)
        if manifest and manifest.get("state") not in (("sealed", "validated") if allow_sealed else ("validated",)):
            raise RuntimeError("search build is not validated")
        meta["_search_build"] = manifest
        if manifest and manifest.get("incremental"):
            meta["complete"] = False
        if manifest and manifest.get("state") == "validated":
            observation = build_path(meta["generation"], manifest["build"]) / "observation.json"
            if (not manifest.get("incremental") or manifest.get("global_complete")) and observation.exists() and json.loads(observation.read_text()).get("state") == "complete":
                meta["complete"] = True
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        meta["_search_error"] = str(error)
        meta["_search_build"] = None
    meta["_search_disabled"] = (generation_path(meta["generation"]) / "search-active.json").exists() and not meta["_search_build"]
    CONTEXT.set((meta["generation"], meta["_search_build"]))
    return meta


@contextlib.contextmanager
def pinned(g, manifest):
    token = CONTEXT.set((g, manifest))
    try:
        yield
    finally:
        CONTEXT.reset(token)


def names(g):
    current = CONTEXT.get()
    manifest = current[1] if current and current[0] == g else active(g)
    return manifest["indexes"] if manifest else None


def save(manifest):
    atomic_json(build_path(manifest["generation"], manifest["build"]) / "build.json", manifest)


def require_metadata(g):
    from .recovery import require_evidence
    from .config import read_generation
    if not read_generation(g).get("metadata_frozen"):
        raise RuntimeError("search builds require frozen metadata")
    return require_evidence(g, "metadata-complete")


def prepare(g, build):
    from .backend import Backend
    from .search_storage import require_qualification, check_runtime
    from .resources import checkpoint
    proof = require_metadata(g)
    qualification = require_qualification(g)
    runtime = Backend(writer=True)
    try:
        runtime_profile = check_runtime(runtime)
    finally:
        runtime.close()
    path = build_path(g, build)
    if (path / "build.json").exists():
        manifest = read(g, build)
        if manifest.get("state") == "failed":
            raise RuntimeError("failed search build is preserved; investigate before creating a new build")
        if manifest["metadata_signature"] != proof["signature"]:
            raise RuntimeError("metadata identity changed")
        return manifest
    samples = qualification["samples"]
    estimated = sum(math.ceil(s["bytes"] / max(s["count"], 1) * s["total"] * 1.5)
                    for s in samples.values())
    # A new build, merge headroom and native snapshot must fit alongside existing data.
    checkpoint(g, large=True, additional=estimated * 2, temporary=estimated // 2)
    shards = {e: max(24 if e == "works" else 1,
                     math.ceil(samples[e]["bytes"] / max(samples[e]["count"], 1) * samples[e]["total"] * 1.5 / (32 * 1024**3)),
                     math.ceil(samples[e]["lucene_docs"] / max(samples[e]["count"], 1) * samples[e]["total"] / 1_000_000_000))
              for e in ENTITIES}
    manifest = dict(generation=g, build=build, state="building", created=time.time(),
                    data_path=runtime_profile["data_path"],
                    projection_signature=signature(), metadata_signature=proof["signature"],
                    qualification=qualification["path"], estimated_bytes=estimated, shards=shards,
                    indexes={e: "oa-search-" + g.replace("_", "-") + "-" + build.replace("_", "-") + "-" + e for e in ENTITIES},
                    entities={e: dict(after="", count=0, done=False) for e in ENTITIES})
    save(manifest)
    return manifest


def checked_documents(rows, documents, entity):
    if len(rows) != len(documents):
        raise RuntimeError("search acknowledgment length mismatch")
    for row, doc in zip(rows, documents):
        expected = search_projection.project(entity, orjson.loads(row["raw"]))["projection_hash"]
        if (not doc.get("found") or doc.get("_id") != row["id"]
                or int(doc.get("_version", -1)) != int(row["version"])
                or doc.get("fields", {}).get("projection_hash") != [expected]):
            raise RuntimeError("search document ID/version/projection mismatch: " + row["id"])


def mget(b, name, keys):
    value = b.os("POST", "/" + name + "/_mget?stored_fields=projection_hash&_source=false", {"ids": keys})
    docs = value.get("docs", [])
    if len(docs) != len(keys) or any(d.get("error") or d.get("_id") != key for d, key in zip(docs, keys)):
        raise RuntimeError("incomplete search verification response")
    return docs


def write_documents(b, g, manifest, entity, rows):
    from .batches import bounded
    name = manifest["indexes"][entity]
    projected_at = time.monotonic()
    records = [(r["id"], int(r["version"]), search_projection.project(entity, orjson.loads(r["raw"]))) for r in rows]
    projection_seconds = time.monotonic()-projected_at
    def encode(record):
        key, version, projection = record
        return (orjson.dumps({"index": {"_index": name, "_id": key, "version": version, "version_type": "external_gte"}})
                + b"\n" + orjson.dumps(projection) + b"\n")
    from concurrent.futures import ThreadPoolExecutor
    profile = generation_path(g) / "search-profile.json"
    concurrency = json.loads(profile.read_text())["bulk_concurrency"] if profile.exists() else b.cfg.get("search_bulk_concurrency", 1)
    concurrency = max(1,min(4,int(concurrency)))
    batches = list(bounded(records,max_rows=max(1,math.ceil(len(records)/concurrency)),max_bytes=config().get("search_bulk_bytes",5*1024**2),encode=encode))
    def write_one(batch):
        with pinned(g,manifest):
            b.bulk(g,entity,batch)
    started = time.monotonic()
    if concurrency == 1:
        for batch in batches:
            write_one(batch)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(write_one,batches))
    bulk_seconds = time.monotonic()-started
    started = time.monotonic()
    checked_documents(rows, mget(b,name,[r["id"] for r in rows]),entity)
    b.search_timings = dict(projection_seconds=projection_seconds,bulk_seconds=bulk_seconds,readback_seconds=time.monotonic()-started)


def verify(b, g, manifest, entity):
    """Both directions, including versions and independently recomputed projections."""
    from .backend import database, sql_string, sql_list
    from .official import source_table
    from .config import read_generation
    from .resources import checkpoint
    name = manifest["indexes"][entity]
    table = source_table(b, read_generation(g), entity)
    from .search_scopes import selection, source_rows
    predicate = selection(g, manifest, entity)
    after, count = "", 0
    while True:
        checkpoint(g)
        rows = source_rows(b, g, entity, after, selection_value=manifest.get("selection"))
        if not rows:
            break
        checked_documents(rows, mget(b, name, [r["id"] for r in rows]), entity)
        after, count = rows[-1]["id"], count + len(rows)
    b.os("POST", "/" + name + "/_refresh")
    pit = b.os("POST", "/" + name + "/_search/point_in_time?keep_alive=10m")["pit_id"]
    reverse, after = 0, ""
    try:
        while True:
            checkpoint(g)
            body = dict(size=1000, pit=dict(id=pit, keep_alive="10m"), sort=[{"id": "asc"}],
                        version=True, _source=False, stored_fields=["projection_hash"], track_total_hits=False)
            if after:
                body["search_after"] = [after]
            result = b.os("POST", "/_search", body)
            pit = result.get("pit_id", pit)
            hits = result["hits"]["hits"]
            if not hits:
                break
            keys = [h["_id"] for h in hits]
            if keys != sorted(set(keys)) or keys[0] <= after or any(h["sort"] != [h["_id"]] for h in hits):
                raise RuntimeError("unordered search identity traversal")
            rows = b.ch(f"SELECT id,version,raw FROM {table} WHERE entity={sql_string(entity)} AND id IN {sql_list(keys)} AND ({predicate}) ORDER BY id FORMAT JSONEachRow")
            checked_documents(rows, [dict(h, found=True) for h in hits], entity)
            reverse, after = reverse + len(hits), keys[-1]
    finally:
        with contextlib.suppress(Exception):
            b.os("DELETE", "/_search/point_in_time", {"pit_id": [pit]})
    if count != reverse or b.os("GET", "/" + name + "/_count")["count"] != count:
        raise RuntimeError("search membership count mismatch")
    return count


def build(g, build):
    from .backend import Backend, database, sql_string
    from .ingest import lock
    from .official import source_table
    from .config import read_generation
    from .resources import checkpoint
    from .search_storage import require_qualification
    with lock(generation_path(g) / "search-controller.lock"):
        manifest = prepare(g, build)
        if manifest["state"] in ("sealed", "validated"):
            return manifest
        b = Backend(writer=True)
        try:
            with pinned(g, manifest):
                for entity in ENTITIES:
                    state = manifest["entities"][entity]
                    name = manifest["indexes"][entity]
                    head = b.http.head(b.cfg["os_url"] + "/" + name)
                    if head.status_code == 404:
                        if state["count"]:
                            raise RuntimeError("checkpoint refers to a missing index")
                        settings = search_projection.mapping(manifest["shards"][entity])
                        settings["settings"]["refresh_interval"] = "-1"
                        b.os("PUT", "/" + name, settings)
                    else:
                        head.raise_for_status()
                    actual = b.os("GET", "/" + name + "/_settings")[name]["settings"]["index"]
                    if state.get("uuid", actual["uuid"]) != actual["uuid"]:
                        raise RuntimeError("build index identity changed")
                    state["uuid"] = actual["uuid"]
                    save(manifest)
                    table = source_table(b, read_generation(g), entity)
                    while not state["done"]:
                        checkpoint(g)
                        rows = b.ch(f"SELECT id,version,raw FROM {table} WHERE entity={sql_string(entity)} AND id>{sql_string(state['after'])} ORDER BY id LIMIT 1000 FORMAT JSONEachRow")
                        if not rows:
                            state["done"] = True
                            save(manifest)
                            break
                        write_documents(b, g, manifest, entity, rows)
                        state.update(after=rows[-1]["id"], count=state["count"] + len(rows))
                        save(manifest)
                    if not state.get("verified"):
                        state["verified"] = verify(b, g, manifest, entity)
                        if state["verified"] != state["count"]:
                            raise RuntimeError("build checkpoint count differs from verified membership")
                        save(manifest)
                    b.os("PUT", "/" + name + "/_settings", {"index.blocks.write": True, "index.refresh_interval": "30s"})
                require_qualification(g)
                if signature() != manifest["projection_signature"]:
                    raise RuntimeError("projection changed during build")
                manifest.update(state="sealed", sealed=time.time())
                save(manifest)
                return manifest
        except BaseException as error:
            from .resources import Paused
            manifest.update(state="building" if isinstance(error, Paused) else "failed",
                            error=repr(error), backend_context=getattr(error, "backend_context", None))
            save(manifest)
            raise
        finally:
            b.close()


def publish(g, build):
    from .backend import Backend
    from .ingest import lock
    from .search_storage import require_qualification
    with lock(generation_path(g) / "search-controller.lock"):
        manifest = read(g, build)
        require_metadata(g)
        require_qualification(g)
        if manifest.get("state") != "validated":
            raise RuntimeError("search publish requires physical, snapshot and API acceptance")
        from .proofs import file_hash
        acceptance = json.loads((build_path(g, build) / "acceptance.json").read_text())
        from .search_acceptance import require_passed
        require_passed(acceptance, g, build)
        if acceptance.get("code") != {p.name: file_hash(p) for p in (ROOT / "openalex_api").glob("*.py")}:
            raise RuntimeError("search API implementation changed after acceptance")
        if not manifest.get("physical_checked") or not manifest.get("snapshot") or not manifest.get("restore_checked"):
            raise RuntimeError("search physical or snapshot recovery acceptance is incomplete")
        b = Backend(writer=True)
        try:
            for e, name in manifest["indexes"].items() if not manifest.get("incremental") else ((s["entity"], s["index"]) for s in manifest["segments"]):
                settings = b.os("GET", "/" + name + "/_settings")[name]["settings"]["index"]
                state = next(s for s in manifest["segments"] if s["index"] == name) if manifest.get("incremental") else manifest["entities"][e]
                if settings["uuid"] != state["uuid"] or settings.get("blocks", {}).get("write") != "true":
                    raise RuntimeError("sealed search index identity changed")
            pointer = generation_path(g) / "search-active.json"
            prior = json.loads(pointer.read_text()) if pointer.exists() else None
            atomic_json(pointer, dict(build=build, previous=prior, published=time.time()))
            return dict(generation=g, search_build=build, published=True)
        finally:
            b.close()


def disable(g):
    from .ingest import lock
    with lock(generation_path(g) / "search-controller.lock"):
        pointer = generation_path(g) / "search-active.json"
        prior = json.loads(pointer.read_text()) if pointer.exists() else None
        atomic_json(pointer, dict(build=None, previous=prior, disabled=time.time()))
        return dict(generation=g, search_ready=False, metadata_unchanged=True)


def validate(g, build):
    import os
    import subprocess
    import sys
    import httpx
    from .backend import Backend
    from .ingest import lock
    from .search_storage import require_qualification, check_physical
    from .resources import checkpoint
    from .search_acceptance import run, require_passed
    from .config import read_generation
    with lock(generation_path(g) / "search-controller.lock"):
        manifest = read(g, build)
        require_metadata(g)
        require_qualification(g)
        if manifest["state"] == "validated":
            return manifest
        if manifest["state"] != "sealed":
            raise RuntimeError("only sealed search builds can be validated")
        path = build_path(g, build)
        b = Backend(writer=True)
        proc = None
        try:
            entries = [(s["entity"], s["index"], s) for s in manifest["segments"]] if manifest.get("incremental") else [(e,n,manifest["entities"][e]) for e,n in manifest["indexes"].items()]
            for entity, name, state in entries:
                checkpoint(g)
                expected = state["expected"] if manifest.get("incremental") else read_generation(g)["expected"][entity]["records"]
                if manifest.get("incremental") and state.get("physical_checked"):
                    actual = b.os("GET", "/" + name + "/_settings")[name]["settings"]["index"]
                    if actual["uuid"] != state["uuid"] or actual.get("blocks", {}).get("write") != "true":
                        raise RuntimeError("reused index is not the sealed validated block")
                    continue
                if not state.get("done") or state.get("count") != state.get("verified") or state["count"] != expected:
                    raise RuntimeError("search entity coverage is incomplete: " + entity)
                b.os("POST", "/" + name + "/_flush?wait_if_ongoing=true")
                closed = b.os("POST", "/" + name + "/_close")
                if not closed.get("acknowledged") or not closed.get("shards_acknowledged"):
                    raise RuntimeError("search index close was not acknowledged")
                try:
                    from .storage import assert_index_closed
                    assert_index_closed(b, name)
                    data_path = Path(manifest.get("data_path", ROOT / "var/data/opensearch")).resolve()
                    if not data_path.is_relative_to(ROOT / "var"):
                        raise RuntimeError("search physical data path escaped project storage")
                    shards = sorted((data_path / "nodes/0/indices" / state["uuid"]).glob("*/index"))
                    if len(shards) != (state["shards"] if manifest.get("incremental") else manifest["shards"][entity]):
                        raise RuntimeError("missing physical search shards: " + entity)
                    checks = {s.parent.name: check_physical(g, s, path / ("physical-" + name + "-" + s.parent.name + ".log")) for s in shards}
                    atomic_json(path / ("physical-" + name + ".json"), checks)
                    state["physical_checked"] = True
                finally:
                    b.os("POST", "/" + name + "/_open")
            manifest["physical_checked"] = True
            save(manifest)
            from .search_scopes import all_indexes
            physical_names = all_indexes(manifest)
            repository = ROOT / "var/backups/opensearch/search-builds"
            repository.mkdir(parents=True, exist_ok=True)
            b.os("PUT", "/_snapshot/oa-search-builds", {"type": "fs", "settings": {"location": str(repository)}})
            snapshot = g.replace("_", "-") + "-" + build.replace("_", "-")
            reply = b.http.get(b.cfg["os_url"] + "/_snapshot/oa-search-builds/" + snapshot)
            if reply.status_code == 404:
                b.os("PUT", "/_snapshot/oa-search-builds/" + snapshot,
                     {"indices": ",".join(physical_names), "include_global_state": False, "partial": False})
            else:
                reply.raise_for_status()
            while True:
                checkpoint(g)
                info = b.os("GET", "/_snapshot/oa-search-builds/" + snapshot)["snapshots"][0]
                if info["state"] == "SUCCESS":
                    if set(info["indices"]) != set(physical_names) or info["shards"]["failed"]:
                        raise RuntimeError("snapshot index coverage differs from the sealed build")
                    break
                if info["state"] not in ("INIT", "STARTED", "IN_PROGRESS"):
                    raise RuntimeError("native search snapshot failed")
                time.sleep(2)
            manifest["snapshot"] = dict(repository="oa-search-builds", name=snapshot)
            save(manifest)
            from .search_incremental import segment_manifest
            restore_segment = min((s for s in manifest["segments"] if s["entity"] == "works"), key=lambda s:s["count"]) if manifest.get("incremental") else None
            entity = "works" if restore_segment else "topics"
            original = restore_segment["index"] if restore_segment else manifest["indexes"][entity]
            restored = original + "-restore-probe"
            b.os("POST", "/_snapshot/oa-search-builds/" + snapshot + "/_restore?wait_for_completion=true",
                 {"indices": original, "include_global_state": False, "include_aliases": False,
                  "rename_pattern": "^" + re.escape(original) + "$", "rename_replacement": restored})
            with pinned(g, manifest):
                restored_manifest = segment_manifest(manifest, restore_segment, restored) if restore_segment else dict(manifest, indexes=manifest["indexes"] | {entity: restored})
                count = verify(b, g, restored_manifest, entity)
            if count != (restore_segment["count"] if restore_segment else manifest["entities"][entity]["count"]):
                raise RuntimeError("native snapshot restore verification failed")
            atomic_json(path / "restore-check.json", dict(entity=entity, count=count, passed=True))
            b.os("DELETE", "/" + restored)
            manifest["restore_checked"] = True
            save(manifest)
            with (path / "acceptance-api.log").open("w") as log:
                proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "openalex_api.api:app", "--host", "127.0.0.1",
                                         "--port", "18090", "--workers", str(config()["api_workers"])], cwd=ROOT,
                    env=os.environ | dict(OPENALEX_GENERATION=g, OPENALEX_SEARCH_BUILD=build, OPENALEX_SEARCH_VALIDATION="1"),
                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            import psutil
            atomic_json(ROOT / "var/run/validation-api-search.json", dict(pid=proc.pid, create_time=psutil.Process(proc.pid).create_time(), generation=g))
            with httpx.Client(timeout=3, trust_env=False) as client:
                for _ in range(90):
                    if proc.poll() is not None:
                        raise RuntimeError("candidate acceptance API exited")
                    try:
                        if client.get("http://127.0.0.1:18090/healthz").is_success:
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(1)
                else:
                    raise RuntimeError("candidate acceptance API startup timed out")
            acceptance = run(g, build, "http://127.0.0.1:18090")
            atomic_json(path / "acceptance.json", acceptance)
            require_passed(acceptance, g, build)
            require_qualification(g)
            manifest.update(state="validated", validated=time.time())
            save(manifest)
            return manifest
        except BaseException as error:
            atomic_json(path / "validation-error.json", dict(error=repr(error),
                        backend_context=getattr(error, "backend_context", None), time=time.time()))
            raise
        finally:
            if proc and proc.poll() is None:
                import signal
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    raise RuntimeError("acceptance API shutdown pending")
            b.close()


def observe(g, build):
    import httpx
    from .resources import checkpoint
    from .config import active as metadata_active
    path = build_path(g, build) / "observation.json"
    state = dict(build=build, seconds=0, state="observing", started=time.time())
    last = time.monotonic()
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{config()['port']}",
                          headers={"X-API-Key": config()["api_keys"][0]}, timeout=config()["query_timeout"] + 5, trust_env=False) as h:
            while state["seconds"] < 86400:
                checkpoint(g)
                pointer = json.loads((generation_path(g) / "search-active.json").read_text())
                if pointer.get("build") != build or metadata_active()["generation"] != g:
                    state.update(state="superseded")
                    return state
                reply = h.get("/status")
                reply.raise_for_status()
                if not reply.json().get("search_ready") or reply.json().get("search_build") != build:
                    raise RuntimeError("published search readiness changed")
                reply = h.get("/v1/works", params=dict(search="climate change", search_field="title", per_page=25, select="id,doi,title"))
                reply.raise_for_status()
                now = time.monotonic()
                state.update(seconds=state["seconds"] + now - last, updated=time.time())
                last = now
                atomic_json(path, state)
                time.sleep(30)
        state.update(state="complete", finished=time.time())
        return state
    except Exception as error:
        from .resources import Paused
        if isinstance(error, Paused):
            state.update(state="paused", error=repr(error))
            raise
        pointer = json.loads((generation_path(g) / "search-active.json").read_text())
        if pointer.get("build") == build:
            prior = (pointer.get("previous") or {}).get("build")
            if prior:
                publish(g, prior)
            else:
                disable(g)
        state.update(state="failed", error=repr(error), metadata_unchanged=True)
        raise
    finally:
        atomic_json(path, state)
