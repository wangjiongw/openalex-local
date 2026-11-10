"""Real API acceptance, optional 24h observation, and controlled publication."""
import os
import time
import shutil
import asyncio

import httpx

from .config import ROOT, config, atomic_json, generation_path
from .earth_control import read_json
from .manticore_build import Client, restore, verify_segment, signature, save, file_hash, physical_check
from .search_versions import read, build_path


def observation_seconds():
    value = config().get("manticore_observation_seconds", 86400)
    if type(value) is not int or value not in (0, 86400):
        raise ValueError("manticore_observation_seconds must be 0 or 86400")
    return value


def fixtures(g, m, path):
    from .earth_delivery import SELECT, write_curl
    from .backend import Backend
    from .model import canonical_id
    # Reuse independently prepared DOI/graph expectations; restrict text fixtures to this build's members.
    origin = ROOT / "reports/earth-delivery-20261105/query-cases/cases.json"
    fixture = read_json(origin)
    groups = set(m["groups"])
    keys = {r["id"] for r in fixture["papers"] if r["group"] in groups}
    c = Client()
    try:
        query_ids = ",".join(k[1:] for k in keys)
        available = c.rows(f"SELECT original_id FROM {m['indexes']['works']} WHERE id IN ({query_ids}) LIMIT {len(keys)}")
        keys &= {r["original_id"] for r in available}
    finally:
        c.close()
    fixture["papers"] = [r for r in fixture["papers"] if r["id"] in keys]
    fixture["cases"] = [r for r in fixture["cases"] if r["name"].split(":")[-1] in keys and not r["name"].startswith("gold-")]
    b = Backend()
    try:
        records = b.details(g, "works", sorted(keys))
    finally:
        b.close()
    for key, record in records.items():
        topics = record.get("topics") or []
        if topics:
            fixture["cases"].append(dict(name="topic:" + key, phase="search", path="/v1/works",
                params=dict(filter="id:" + key + ",topic:" + canonical_id(topics[0]["id"]), search_scope="earth_sciences", select=SELECT),
                expected=dict(shape="search", ids=[key])))
        keywords = record.get("keywords") or []
        if keywords:
            fixture["cases"].append(dict(name="keywords:" + key, phase="search", path="/v1/works",
                params=dict(search=keywords[0]["display_name"], search_field="title_abstract_keywords",
                            filter="id:" + key, search_scope="earth_sciences", select=SELECT),
                expected=dict(shape="search", ids=[key])))
    for key, record in records.items():
        year, oa = record.get("publication_year"), (record.get("open_access") or {}).get("is_oa")
        if isinstance(year, int) and isinstance(oa, bool):
            fixture["cases"].append(dict(name="scoped-filter:" + key, phase="search", path="/v1/works",
                params=dict(filter=f"year:{year},is_oa:{str(oa).lower()}", search_scope="earth_sciences", select=SELECT),
                expected=dict(shape="search", min_results=1, year=year, oa=oa)))
            break
    for entity in ("topics", "sources"):
        for record in records.values():
            item = (record.get("topics") or [None])[0] if entity == "topics" else (record.get("primary_location") or {}).get("source")
            if item and item.get("display_name"):
                fixture["cases"].append(dict(name="catalog-" + entity, phase="search", path="/v1/" + entity,
                    params=dict(search=item["display_name"], search_field="title", select="id,display_name"),
                    expected=dict(shape="search", contains=canonical_id(item["id"]))))
                break
    # These cases exercise retrieval without a document-ID shortcut.
    selected_fields = SELECT + ",topics,primary_topic,locations,primary_location"
    seen = set()
    for key, record in records.items():
        if len(seen) >= 24:
            break
        topic = (record.get("topics") or [{}])[0].get("id")
        primary_topic = (record.get("primary_topic") or {}).get("id")
        source = ((record.get("primary_location") or {}).get("source") or {}).get("id")
        for field, value in (("topic", topic), ("primary_topic", primary_topic), ("source", source), ("primary_source", source)):
            if not value:
                continue
            value = canonical_id(value)
            if (field, value) in seen or sum(f == field for f, _ in seen) >= 3:
                continue
            seen.add((field, value))
            fixture["cases"].append(dict(name="real-" + field.replace("_", "-") + ":" + value, phase="search",
                path="/v1/works", params=dict(filter=field+":"+value, search_scope="earth_sciences", count="exact",
                select=selected_fields), expected=dict(shape="search", min_results=1, **{field: value})))
    scoped = next((c for c in fixture["cases"] if c["name"].startswith("scoped-filter:")), None)
    if scoped:
        fixture["cases"].append(dict(scoped, name="real-year-oa:combined", params=scoped["params"] | {"count": "exact"}))
    for kind in ("title", "author", "title-author-year", "title-year-oa"):
        for case in [c for c in fixture["cases"] if c["name"].startswith(kind+":")][:3]:
            fixture["cases"].append(dict(case, name="real-"+case["name"], params=case["params"] | {"count": "exact"}))
    for case in [c for c in fixture["cases"] if c["name"].startswith("title:")][:3]:
        key = case["name"].split(":", 1)[1]
        record = records[key]
        for field, value in (("topic", (record.get("topics") or [{}])[0].get("id")),
                             ("source", ((record.get("primary_location") or {}).get("source") or {}).get("id"))):
            if value:
                value = canonical_id(value)
                fixture["cases"].append(dict(case, name=f"real-title-{field}:{key}",
                    params=case["params"] | dict(filter=field+":"+value, count="exact", select=selected_fields),
                    expected=case["expected"] | {field: value}))
    for case in [c for c in fixture["cases"] if c["name"].startswith("keywords:")][:3]:
        params = {k: v for k, v in case["params"].items() if k != "filter"}
        fixture["cases"].append(dict(case, name="real-"+case["name"], params=params | {"count": "exact"},
                                    expected=dict(shape="search", min_results=1)))
    fixture["strict_performance"] = True
    fixture.update(generation=g, search_build=m["build"], snapshot_complete=True, snapshot_blocks=5015,
                   collection_signature=m["collection_signature"])
    if len(keys) < 10 or not fixture["cases"]:
        raise RuntimeError("insufficient actual acceptance samples")
    atomic_json(path / "cases.json", fixture)
    write_curl(path, fixture)
    return fixture


def qualify(g, build):
    from . import manage
    from .earth_delivery import check, validate_response
    required_seconds = observation_seconds()
    path = build_path(g, build)
    m = read(g, build)
    from .manticore_pipeline import wait_parent
    if m.get("parent_artifact_signature"):
        wait_parent(g, m, published=True)
    if m["state"] not in ("sealed", "validated"):
        raise RuntimeError("only sealed builds can be qualified")
    output = path / "qualification"
    if output.exists() and any(f.is_file() for f in output.iterdir()):
        archive = output / "attempts" / str(time.time_ns())
        archive.mkdir(parents=True)
        for f in list(output.iterdir()):
            if f.is_file():
                shutil.move(str(f), archive / f.name)
        if (path / "observation.json").exists():
            shutil.copy2(path / "observation.json", archive / "observation.json")
    atomic_json(path / "observation.json", dict(state="verifying", build=build, started=time.time(), errors=0))
    crash_path = ("reports/manticore-release-20261108/crash-recovery-native.json" if m.get("physical_layout") == "native_v2"
                  else "reports/manticore-implementation-20261106/crash-recovery.json")
    crash = read_json(ROOT / crash_path)
    binary = ROOT / "var/engines/manticore-29.9.0/usr/bin/searchd"
    if not crash.get("passed") or crash["projection_signature"] != signature(m.get("physical_layout", "legacy")) or crash["binary_sha256"] != file_hash(binary):
        raise RuntimeError("current engine/projection must pass isolated crash/replay/restore checks")
    c = Client(timeout=300)
    try:
        for segment in m["segments"]:
            print("Verifying persisted snapshot: " + segment["index"], flush=True)
            if not segment.get("sealed") or not segment.get("restore_verified"):
                raise RuntimeError("unverified segment")
            restore(c, path, segment)
            if not segment.get("physical_verified"):
                physical_check(path, segment)
        save(m)
    finally:
        c.close()
    service = "validation-api-" + g
    if manage.identity(manage.state_path(service)):
        raise RuntimeError("validation API is in use; stop the previous validation service first")
    os.environ["OPENALEX_SEARCH_BUILD"] = build
    os.environ["OPENALEX_SEARCH_VALIDATION"] = "1"
    manage.start(service)
    output = path / "qualification"
    output.mkdir(exist_ok=True)
    fixture = fixtures(g, m, output)
    print(f"Starting API acceptance: {len(fixture['cases'])} cases; concurrency matrix and 30-minute load", flush=True)
    observation = dict(state="load_testing", started=time.time(), build=build, projection_signature=signature(m.get("physical_layout", "legacy")),
                       cross_pod_tested=False, errors=0, required_seconds=required_seconds,
                       long_observation_skipped=required_seconds == 0, crash_recovery=crash)
    atomic_json(path / "observation.json", observation)
    try:
        report = check(output / "cases.json", output, "http://127.0.0.1:18088", "all", 1800)
        if not report["passed"]:
            raise RuntimeError("API functional/performance gate failed; inspect qualification/acceptance.json")
        engine_state = read_json(manage.state_path("manticore"))
        engine_identity = {k: engine_state[k] for k in ("child_pid", "child_create_time")}
        observation.update(state="observing", observation_started=time.time(), updated=time.time(), samples=0, engine_identity=engine_identity)
        print("Load gates passed; " + ("starting uninterrupted 24-hour observation" if required_seconds else
              "24-hour observation disabled; mixed build/load test follows publication"), flush=True)
        atomic_json(path / "observation.json", observation)
        observation.update(mixed_load_required=False, mixed_load_phase="after_publication")
        atomic_json(path / "observation.json", observation)
        cfg = config()
        with httpx.Client(base_url="http://127.0.0.1:18088", headers={"X-API-Key": cfg["api_keys"][0]},
                          timeout=cfg["query_timeout"] + 5, trust_env=False) as client:
            cases = fixture["cases"]
            while time.time() - observation["observation_started"] < required_seconds:
                if signature(m.get("physical_layout", "legacy")) != observation["projection_signature"]:
                    raise RuntimeError("projection/query changed during observation")
                if any(file_hash(ROOT / "openalex_api" / name) != checksum for name, checksum in report["code"].items()):
                    raise RuntimeError("application changed during observation; repeat qualification")
                engine_state = read_json(manage.state_path("manticore"))
                if any(engine_state.get(k) != v for k, v in engine_identity.items()):
                    raise RuntimeError("engine restarted during observation; repeat qualification")
                case = cases[observation["samples"] % len(cases)]
                validate_response(case, client.get(case["path"], params=case["params"]), fixture["records"])
                state = client.get("/status")
                state.raise_for_status()
                if state.json()["search_build"] != build or not state.json()["search_ready"]:
                    raise RuntimeError("candidate search became unavailable")
                observation.update(samples=observation["samples"] + 1, updated=time.time(),
                                   elapsed_seconds=time.time() - observation["observation_started"])
                atomic_json(path / "observation.json", observation)
                time.sleep(30)
        c = Client(timeout=300)
        try:
            for segment in m["segments"]:
                verify_segment(c, path, segment)
        finally:
            c.close()
        observation.update(state="complete", completed=time.time(), elapsed_seconds=time.time() - observation["observation_started"])
        atomic_json(path / "observation.json", observation)
        m.update(state="validated", validated_at=time.time())
        save(m)
        manage.stop(service)
    except Exception as error:
        observation.update(state="failed", error=str(error), errors=observation["errors"] + 1, updated=time.time())
        atomic_json(path / "observation.json", observation)
        raise


def publish(g, build):
    from .ingest import lock
    with lock(generation_path(g) / "search-controller.lock"):
        return _publish(g, build)


def _publish(g, build):
    m = read(g, build)
    path = build_path(g, build)
    observation = read_json(path / "observation.json")
    acceptance = read_json(path / "qualification/acceptance.json")
    if m.get("parent_artifact_signature"):
        from .manticore_pipeline import wait_parent
        wait_parent(g, m, published=True)
    observation_passed = observation.get("elapsed_seconds", 0) >= 86400 or (
        observation_seconds() == 0 and observation.get("long_observation_skipped") is True
        and observation.get("required_seconds") == 0)
    if (m["state"] != "validated" or observation["state"] != "complete" or observation.get("errors")
            or not observation_passed or not acceptance.get("passed")
            or not acceptance.get("performance_passed") or observation["projection_signature"] != signature(m.get("physical_layout", "legacy"))):
        raise RuntimeError("publication requires the configured observation policy and passed API/load/restore gates")
    if any(file_hash(ROOT / "openalex_api" / name) != checksum for name, checksum in acceptance["code"].items()):
        raise RuntimeError("application changed after qualification; repeat qualification")
    if observation.get("mixed_load_required"):
        mixed = read_json(path / "qualification/mixed-load.json")
        if not mixed.get("passed") or mixed.get("build_records_added", 0) <= 0:
            raise RuntimeError("publication requires passed concurrent build/load gate")
    from .manticore_pipeline import artifact_signature
    if m.get("rebuild_source"):
        original = read(g, m["rebuild_source"])
        proof = m.get("source_reconciliation", {})
        if (not proof.get("passed") or proof.get("source") != original["build"]
                or m.get("source_artifact_signature") != artifact_signature(original)):
            raise RuntimeError("publication requires exact source member reconciliation")
    if m.get("physical_layout") == "native_v2":
        from .manticore_formats import check_indexes
        c = Client(timeout=300)
        try:
            for segment in m["segments"]:
                if not segment.get("physical_verified") or not segment.get("restore_verified"):
                    raise RuntimeError("native snapshot has not passed physical/recovery checks")
                check_indexes(c, segment["index"])
        finally:
            c.close()
    pointer = generation_path(g) / "search-active.json"
    previous = read_json(pointer) if pointer.exists() else None
    if previous and previous.get("build") == build:
        smoke(g, m)
        return
    old_cursor = capture_cursor(previous)
    receipt = dict(build=build, previous=previous, started=time.time(), state="switching")
    atomic_json(path / "publication.json", receipt)
    atomic_json(pointer, dict(build=build, engine="manticore", previous=previous, published=time.time()))
    try:
        smoke(g, m, old_cursor)
        if previous and previous.get("build"):
            old = read(g, previous["build"])
            atomic_json(build_path(g, old["build"]) / "published.json", dict(artifact_signature=artifact_signature(old)))
        atomic_json(path / "published.json", dict(artifact_signature=artifact_signature(m), published=time.time()))
        receipt.update(state="published", completed=time.time())
    except Exception as error:
        atomic_json(pointer, previous or dict(build=None))
        receipt.update(state="rolled_back", error=str(error), completed=time.time())
        raise
    finally:
        atomic_json(path / "publication.json", receipt)
    print("Published validated local search; cross-Pod execution is tracked separately.", flush=True)


def capture_cursor(previous):
    if not previous or not previous.get("build"):
        return None
    params = dict(search="climate", search_field="title", search_scope="earth_sciences",
                  per_page=1, select="id,doi,title", cursor="*")
    cfg = config()
    with httpx.Client(base_url=f"http://127.0.0.1:{cfg['port']}",
                      headers={"X-API-Key": cfg["api_keys"][0]}, timeout=cfg["query_timeout"]+5, trust_env=False) as client:
        response = client.get("/v1/works", params=params)
        response.raise_for_status()
        value = response.json()
        if value["meta"]["search_build"] != previous["build"]:
            raise RuntimeError("production pointer changed before publication")
        cursor = value["meta"].get("next_cursor")
        if not cursor:
            raise RuntimeError("old production cursor smoke fixture has no continuation")
        return dict(params=params | {"cursor":cursor}, build=previous["build"],
                    ids=[r["id"] for r in value["results"]])


def smoke(g, m, old_cursor=None):
    from .earth_delivery import validate_response
    cfg = config()
    fixture = read_json(build_path(g, m["build"]) / "qualification/cases.json")
    with httpx.Client(base_url=f"http://127.0.0.1:{cfg['port']}",
                      headers={"X-API-Key": cfg["api_keys"][0]}, timeout=cfg["query_timeout"]+5, trust_env=False) as client:
        state = client.get("/status")
        state.raise_for_status()
        if not state.json().get("search_ready") or state.json().get("search_build") != m["build"]:
            raise RuntimeError("published search is not ready")
        seen = set()
        for case in fixture["cases"]:
            kind = case["name"].split(":")[0]
            if kind in seen:
                continue
            seen.add(kind)
            response = client.get(case["path"], params=case["params"])
            validate_response(case, response, fixture["records"])
            meta = response.json().get("meta", {})
            if meta.get("search_build") and meta["search_build"] != m["build"]:
                raise RuntimeError("published request used a different build")
            if case["path"] == "/v1/works" and meta.get("search_build"):
                if meta.get("indexed_entities") != m["entities"]["works"]["count"]:
                    raise RuntimeError("published work coverage differs from sealed candidate")
        if old_cursor:
            response = client.get("/v1/works", params=old_cursor["params"])
            response.raise_for_status()
            value = response.json()
            if (value["meta"]["search_build"] != old_cursor["build"] or not value["results"]
                    or set(old_cursor["ids"]) & {r["id"] for r in value["results"]}):
                raise RuntimeError("old published cursor failed after switch")
