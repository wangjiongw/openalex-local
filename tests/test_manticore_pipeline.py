import copy
import json

import pytest

from openalex_api.manticore_api import native_filters, search_request
from openalex_api.query import query


def test_id_pruning_preserves_entity_and_boolean_boundaries():
    assert native_filters("works", {"term": {"id": "W123"}}) == [{"equals": {"id": 123}}]
    assert native_filters("works", {"term": {"id": "S123"}}) == []
    assert native_filters("works", {"term": {"id": "W"+str(2**63)}}) == []
    assert native_filters("works", {"bool": {"must_not": [{"term": {"id": "W123"}}]}}) == []
    assert native_filters("works", {"nested": {"path": "authorships", "query": {"term": {"id": "W123"}}}}) == []
    body = query("works", filters="id:W1|W2,topic:T1")
    native = search_request("works", body, ["fixture"], 25)
    assert len(native["query"]["bool"]["must"]) == 2
    assert {"in": {"id": [1, 2]}} in native["query"]["bool"]["must"]
    assert "oa_predicate" in native["expressions"]


def test_partial_or_is_not_pruned():
    node = {"bool": {"should": [{"term": {"id": "W1"}}, {"term": {"topic_id": "T1"}}]}}
    assert native_filters("works", node) == []


def test_batch_parallel_projection_is_identical():
    from openalex_api.manticore_build import prepare_document
    from openalex_api.manticore_engine import project
    from openalex_api.semantics import digest
    record = dict(id="W1", title="Climate change", publication_year=2020)
    row = dict(id="W1", raw=json.dumps(record), version=7, digest=digest(record))
    member = dict(source_version=7, raw_digest=row["digest"], reasons=["topic"])
    assert prepare_document(("works", "group", row, member)) == project("works", row, "group", ["topic"])
    with pytest.raises(RuntimeError, match="mismatch"):
        prepare_document(("works", "group", row, member | {"source_version": 8}))


def test_parent_artifact_ignores_state_but_not_data():
    from openalex_api.manticore_pipeline import artifact_signature
    m = dict(generation="g", build="b", projection_signature="p", metadata_signature="m", collection_signature="c",
             segments=[dict(count=10, snapshot={"files": [{"sha256": "a"}]})], state="sealed")
    assert artifact_signature(m) == artifact_signature(m | {"state": "validated", "updated": 1})
    changed = copy.deepcopy(m); changed["segments"][0]["count"] = 11
    assert artifact_signature(m) != artifact_signature(changed)


def test_real_filter_response_checks_source_fields():
    import httpx
    from openalex_api.earth_delivery import validate_response
    case = {"expected": {"shape": "search", "topic": "T1", "min_results": 1, "count": 1}}
    good = {"results": [{"id": "W1", "topics": [{"id": "https://openalex.org/T1"}]}],
            "meta": {"count": 1, "count_relation": "eq"}}
    validate_response(case, httpx.Response(200, json=good), {})
    with pytest.raises(AssertionError):
        validate_response(case, httpx.Response(200, json=good | {"results": [{"id": "W1", "topics": []}]}), {})


def test_parent_observation_requires_live_owner_engine_code_and_fresh_heartbeat(monkeypatch, tmp_path):
    import time
    from openalex_api import manticore_pipeline as p, manage, manticore_build
    manifest = dict(state="sealed")
    observation = dict(state="observing", updated=time.time(), engine_identity={"child_pid": 12})
    report = dict(passed=True, code={"api.py": "same"})
    engine = dict(child_pid=12)
    monkeypatch.setattr(p, "build_path", lambda *args: tmp_path)
    monkeypatch.setattr(manage, "state_path", lambda x: tmp_path/x)
    monkeypatch.setattr(manage, "identity", lambda p: True)
    monkeypatch.setattr(manticore_build, "file_hash", lambda p: "same")
    def read(path):
        return {"build.json": manifest, "observation.json": observation, "acceptance.json": report,
                "manticore": engine}[path.name]
    monkeypatch.setattr(p, "read_json", read)
    assert p.parent_ready("g", "parent")
    engine['child_pid'] = 13
    assert not p.parent_ready("g", "parent")
    engine['child_pid'] = 12
    observation['updated'] -= 200
    assert not p.parent_ready("g", "parent")
    observation.update(updated=time.time(), state="failed")
    assert not p.parent_ready("g", "parent")
    manifest["state"] = "validated"
    assert not p.parent_ready("g", "parent")
    observation["state"] = "complete"
    assert p.parent_ready("g", "parent")


def test_request_manifest_cache_follows_pointer_and_replacement(monkeypatch, tmp_path):
    from openalex_api import search_versions as s, manticore_build
    from openalex_api.config import atomic_json
    monkeypatch.setattr(s, "generation_path", lambda g: tmp_path)
    monkeypatch.setattr(s, "signature", lambda *a: "projection")
    monkeypatch.setattr(manticore_build, "signature", lambda *a: "manticore")
    calls = []
    def read(g, build):
        calls.append(build)
        return json.loads((s.build_path(g, build) / "build.json").read_text())
    monkeypatch.setattr(s, "read", read)
    s._request_manifest.cache_clear()
    for build in ("first", "second"):
        atomic_json(s.build_path("g", build) / "build.json", dict(build=build, state="validated"))
    pointer = tmp_path / "search-active.json"
    atomic_json(pointer, dict(build="first"))
    first = s.request_manifest("g")
    assert s.request_manifest("g") is first and calls == ["first"]
    atomic_json(pointer, dict(build="second"))
    assert s.request_manifest("g")["build"] == "second"
    assert s.request_manifest("g", "first") is first
    atomic_json(s.build_path("g", "first") / "build.json", dict(build="first", state="building"))
    with pytest.raises(RuntimeError, match="not sealed"):
        s.request_manifest("g", "first")
    s._request_manifest.cache_clear()


def test_parent_wait_saves_before_waiting_and_rejects_changed_artifact(monkeypatch, tmp_path):
    from openalex_api import manticore_pipeline as p, manticore_build
    parent = dict(generation="g", build="parent", projection_signature="p", metadata_signature="m",
                  collection_signature="c", segments=[])
    child = dict(build="child", parent="parent", entities={}, parent_artifact_signature=p.artifact_signature(parent))
    monkeypatch.setattr(p, "build_path", lambda *args: tmp_path)
    monkeypatch.setattr(p, "read_json", lambda path: parent)
    readiness = iter([False, True])
    monkeypatch.setattr(p, "parent_ready", lambda *args: next(readiness))
    events = []
    monkeypatch.setattr(manticore_build, "save", lambda m: events.append("saved"))
    monkeypatch.setattr(p.time, "sleep", lambda seconds: events.append("waited"))
    p.wait_parent("g", child)
    assert events == ["saved", "waited"]
    parent["segments"].append(dict(count=1))
    with pytest.raises(RuntimeError, match="artifact changed"):
        p.wait_parent("g", child)


@pytest.mark.parametrize("seconds", [0, 86400])
def test_publication_requires_successful_mixed_load(monkeypatch, tmp_path, seconds):
    from openalex_api import manticore_qualification as q
    monkeypatch.setattr(q, "build_path", lambda *args: tmp_path)
    monkeypatch.setattr(q, "generation_path", lambda g: tmp_path)
    m = dict(state="validated", generation="g", build="child", projection_signature="sig",
             metadata_signature="m", collection_signature="c", segments=[])
    monkeypatch.setattr(q, "read", lambda *args: m)
    monkeypatch.setattr(q, "signature", lambda *a: "sig")
    monkeypatch.setattr(q, "config", lambda: {"manticore_observation_seconds": seconds})
    monkeypatch.setattr(q, "smoke", lambda *a: None)
    monkeypatch.setattr(q, "capture_cursor", lambda *a: None)
    mixed = dict(passed=False, build_records_added=10)
    observation = dict(state="complete", elapsed_seconds=seconds, required_seconds=seconds,
                       long_observation_skipped=seconds == 0, projection_signature="sig", mixed_load_required=True)
    acceptance = dict(passed=True, performance_passed=True, code={})
    def read(path):
        return {"observation.json": observation, "acceptance.json": acceptance, "mixed-load.json": mixed}[path.name]
    monkeypatch.setattr(q, "read_json", read)
    with pytest.raises(RuntimeError, match="concurrent build/load"):
        q.publish("g", "child")
    assert not (tmp_path / "search-active.json").exists()
    mixed.update(passed=True, build_records_added=0)
    with pytest.raises(RuntimeError, match="concurrent build/load"):
        q.publish("g", "child")
    mixed["build_records_added"] = 10
    acceptance["performance_passed"] = False
    with pytest.raises(RuntimeError, match="API/load/restore"):
        q.publish("g", "child")
    acceptance["performance_passed"] = True
    if seconds == 0:
        observation["long_observation_skipped"] = False
        with pytest.raises(RuntimeError, match="observation policy"):
            q.publish("g", "child")
        observation["long_observation_skipped"] = True
        monkeypatch.setattr(q, "config", lambda: {"manticore_observation_seconds": 86400})
        with pytest.raises(RuntimeError, match="observation policy"):
            q.publish("g", "child")
        monkeypatch.setattr(q, "config", lambda: {"manticore_observation_seconds": 0})
    q.publish("g", "child")
    assert json.loads((tmp_path / "search-active.json").read_text())["build"] == "child"


def test_observation_policy_defaults_and_validation(monkeypatch):
    from openalex_api import manticore_qualification as q
    values = {}
    monkeypatch.setattr(q, "config", lambda: values)
    assert q.observation_seconds() == 86400
    values["manticore_observation_seconds"] = 0
    assert q.observation_seconds() == 0
    for value in (False, -1, 30, "0"):
        values["manticore_observation_seconds"] = value
        with pytest.raises(ValueError):
            q.observation_seconds()


def test_load_balances_kinds_and_counts_failures(monkeypatch):
    import asyncio
    import httpx
    from openalex_api import earth_delivery as d
    calls=[]
    class Client:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, path, params):
            calls.append(path)
            return httpx.Response(504 if path=='/bad' else 200, json={"results": []})
    monkeypatch.setattr(d.httpx, "AsyncClient", Client)
    cases=[dict(name="fast:"+str(n), path="/ok", params={}, expected={"shape":"search"}) for n in range(10)]
    cases.append(dict(name="slow:one",path="/bad",params={},expected={"shape":"search"}))
    result=asyncio.run(d.load_cases("url", cases, {}, 1, 0, {}, min_per_kind=3))
    assert result['requests']==6 and result['errors']==3
    assert calls==['/ok','/bad']*3
    assert result['by_kind']['slow']['requests']==3


def test_tuning_requires_sustained_integrity_restore_and_speed(monkeypatch, tmp_path):
    from scripts import benchmark_manticore_build as b
    monkeypatch.setattr(b, "ROOT", tmp_path)
    monkeypatch.setattr(b, "signature", lambda *a: "sig")
    row = dict(batch_rows=2000, projection_workers=2, rows=100000, seconds=150, restored=True)
    report = dict(passed=True, projection_signature="sig", selected=row,
                  sustained=[row], sustained_seconds=1800, matrix=[dict(rows_per_second=500)])
    assert b.activate_tuning(report)["passed"]
    row["seconds"] = 200
    assert not b.activate_tuning(report)["passed"]
    row["restored"] = False
    with pytest.raises(RuntimeError, match="restoration"):
        b.activate_tuning(report)


def test_continuation_does_not_restart_completed_or_failed_children(monkeypatch, tmp_path):
    from openalex_api import manticore_pipeline as p, manage
    from openalex_api.config import atomic_json
    monkeypatch.setattr(p, "build_path", lambda *args: tmp_path)
    calls = []
    monkeypatch.setattr(manage, "start", lambda *args: calls.append(args))
    monkeypatch.setattr(p, "parent_ready", lambda *a, **k: True)
    parent = dict(build="parent", entities={"works": {"count": 20000000}}, earth_expected=76850149)
    atomic_json(tmp_path / "build.json", dict(parent="parent", state="validated"))
    assert p.continue_earth("g", parent) == "earth_full_mc_v1" and not calls
    atomic_json(tmp_path / "build.json", dict(parent="parent", state="failed"))
    with pytest.raises(RuntimeError, match="child failed"):
        p.continue_earth("g", parent)
    assert not calls


@pytest.mark.parametrize("field,broad", [("title", False), ("author", False), ("title_abstract_keywords", True), ("all", True)])
def test_broad_text_thread_allocation_preserves_query(field, broad):
    from openalex_api.manticore_query import request_body
    body = query("works", "Geology", field)
    expected = request_body(body, ["fixture"], 25)
    actual = search_request("works", body, ["fixture"], 25)
    assert actual["options"].pop("threads", None) == (32 if broad else None)
    expected["_source"] = ["original_id", "group_name"]
    assert actual == expected


def test_title_content_does_not_request_broad_text_threads():
    body = query("works", "keywords abstract @abstract_en", "title")
    assert "threads" not in search_request("works", body, ["fixture"], 25)["options"]
