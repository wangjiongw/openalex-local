import copy
import hashlib
import json
import os
import uuid

import pytest

from openalex_api import manticore_engine as engine, manticore_query as compiler
from openalex_api.query import query
from openalex_api.official_text import build


def row(n=1, **fields):
    raw = json.dumps(dict(id=f"W{n}", title="Climate change in the oceans", **fields))
    from openalex_api.semantics import digest
    return dict(id=f"W{n}", raw=raw, version=1, digest=digest(json.loads(raw)))


def test_projection_preserves_source_hash_and_handles_invalid_abstract():
    r = row(abstract_inverted_index={"bad": [-1]}, open_access={"is_oa": False},
            primary_location={"source": {"id": "S9", "display_name": "Earth Journal"}})
    key, doc = engine.project("works", r, "01_core_journals")
    assert key == 1 and doc["source_hash"] == r["digest"]
    assert doc["abstract"] == "" and doc["attrs"]["invalid_abstract"]
    assert doc["attrs"]["source_id"] == ["S9"]
    assert doc["attrs"]["present"]["is_oa"] == 1 and doc["is_oa"] == 0
    assert "publication_year" not in doc["attrs"]["present"]
    assert r["raw"] == json.dumps(json.loads(r["raw"]))


def test_compiler_rejects_unsupported_operators_and_escapes_text():
    with pytest.raises(ValueError, match="fuzzy"):
        compiler.request_body({"query": {"multi_match": {"query": "ocean", "fields": ["name"], "fuzziness": 1}}}, ["test"], 10)
    assert r"\@" in compiler.literal_text("@name hello")
    with pytest.raises(ValueError):
        engine.checked_name("test; DROP TABLE other")


def test_topic_catalog_accepts_string_keywords():
    _, doc = engine.project("topics", dict(raw=json.dumps(dict(id="T1", display_name="Climate",
        keywords=["ocean", "atmosphere"])), version=1))
    assert doc["name"] == "climate" and doc["keywords"] == ""


@pytest.fixture
def live():
    url = os.environ.get("MANTICORE_TEST_URL")
    if not url:
        pytest.skip("set MANTICORE_TEST_URL for isolated engine integration")
    c = engine.Client(url)
    name = "oa_test_" + uuid.uuid4().hex[:16]
    engine.create(c, name)
    try:
        yield c, name
    finally:
        c.sql("DROP TABLE " + name)
        c.close()


def test_live_authorship_filters_boolean_pagination_and_keywords(live):
    c, name = live
    authors = [dict(author=dict(id="A1", display_name="John Smith"), institutions=[dict(id="I1")]),
               dict(author=dict(id="A2", display_name="Alice Jones"), institutions=[dict(id="I2")])]
    rows = [row(1, publication_year=2020, open_access={"is_oa": True}, authorships=authors,
                topics=[{"id": "T1"}], keywords=[{"id": "keywords/1", "display_name": "permafrost"}]),
            row(2, publication_year=2021, open_access={"is_oa": False}), row(3)]
    c.bulk(name, [engine.project("works", r) for r in rows])
    def find(q, limit=10, scroll=None):
        return c.search(compiler.request_body(q, [name], limit, scroll=scroll))
    def ids(q):
        return [h["_source"]["original_id"] for h in find(q)["hits"]["hits"]]
    assert ids(query("works", "climate", "title", "year:2020,is_oa:true,topic:T1")) == ["W1"]
    assert ids(query("works", "", "all", author_search="John Smith")) == ["W1"]
    assert ids(query("works", "", "all", author_search="John Jones")) == []
    assert ids(query("works", "", "all", "author:A1,institution:I2", author_search="John Smith")) == []
    assert ids(query("works", "", "all", "author:A1,institution:I1", author_search="John Smith")) == ["W1"]
    assert ids(query("works", "", "all", "is_oa:false")) == ["W2"]
    from openalex_api.manticore_api import search_request
    native = c.search(search_request("works", query("works", "", "all", "is_oa:false"), [name], 10))
    assert [h["_source"]["original_id"] for h in native["hits"]["hits"]] == ["W2"]
    for filters, expected_ids in (("id:W1|W2,topic:T1", ["W1"]), ("id:!W1", ["W2", "W3"]),
                                  ("year:2020..2021,is_oa:false", ["W2"]), ("id:W2,topic:T1", [])):
        actual = c.search(search_request("works", query("works", filters=filters), [name], 10))
        assert actual["hits"]["total"] == len(expected_ids)
        assert [h["_source"]["original_id"] for h in actual["hits"]["hits"]] == expected_ids
    assert ids(query("works", "", "all", "is_oa:null")) == ["W3"]
    assert ids(query("works", "", "all", "is_oa:!null")) == ["W1", "W2"]
    assert ids(build("works", {"search.title_abstract_keywords": "permafrost"}, [])) == ["W1"]
    for _ in range(2):
        assert len(ids(build("works", {"search.title": "ocean"}, []))) == 3
        assert ids(build("works", {"search.title.exact": "ocean"}, [])) == []
        assert len(ids(build("works", {"search.title.exact": "oceans"}, []))) == 3
        assert ids(build("works", {"search.title": "climate NOT ocean"}, [])) == []
        c.sql("FLUSH RAMCHUNK " + name)
    q = query("works", "", "all", "", sort="id:asc")
    first = find(q, 2)
    second = find(q, 2, first["scroll"])
    assert first["hits"]["total"] == 3
    assert [h["_source"]["original_id"] for h in first["hits"]["hits"] + second["hits"]["hits"]] == ["W1", "W2", "W3"]


def test_live_snapshot_restore_reconciles_all_receipts(live, tmp_path):
    from openalex_api.manticore_build import write_batch, seal, restore
    c, name = live
    segment = dict(index=name, entity="works", count=0, batches=[])
    write_batch(c, None, tmp_path, segment, [row(10), row(11)])
    seal(c, tmp_path, segment)
    assert segment["restore_verified"]
    c.sql("DROP TABLE " + name)
    restore(c, tmp_path, segment)
    assert int(c.rows(f"SELECT count(*) AS n FROM {name}")[0]["n"]) == 2


def test_live_cjk_accent_and_exact_phrases(live):
    c, name = live
    c.bulk(name, [engine.project("works", row(1, authorships=[{"author": {"id": "A1", "display_name": "José García"}}])),
                 engine.project("works", dict(raw=json.dumps(dict(id="W2", title="气候变化与海洋环流")), version=1))])
    def ids(body):
        return [h["_source"]["original_id"] for h in c.search(compiler.request_body(body, [name], 10))["hits"]["hits"]]
    for _ in range(2):
        assert ids(query("works", "海洋环流", "title")) == ["W2"]
        assert ids(query("works", "", "all", author_search="Jose Garcia")) == ["W1"]
        assert ids(build("works", {"search.title": '"climate change"'}, [])) == ["W1"]
        assert ids(build("works", {"search.title": '"change climate"'}, [])) == []
        c.sql("FLUSH RAMCHUNK " + name)


def test_live_catalog_exact_name_precedes_repeated_terms_and_scrolls(live):
    from openalex_api.manticore_api import search_request
    c, name = live
    rows = [dict(raw=json.dumps(dict(id="S" + str(n), display_name=title)), version=1)
            for n, title in [(1, "Geology Geology Geology"), (2, "Journal of Geology"), (99, "Geology")]]
    c.bulk(name, [engine.project("sources", r) for r in rows])
    body = query("sources", "Geology", "title")
    first = c.search(search_request("sources", body, [name], 1))
    assert first["hits"]["hits"][0]["_source"]["original_id"] == "S99"
    second = c.search(search_request("sources", body, [name], 5, scroll=first["scroll"]))
    assert len(second["hits"]["hits"]) == 2
    assert "S99" not in [h["_source"]["original_id"] for h in second["hits"]["hits"]]


def test_live_api_pins_cursor_and_returns_raw_records(live, monkeypatch):
    from fastapi.testclient import TestClient
    from unittest.mock import Mock
    from openalex_api import api, manticore_api, search_versions
    from openalex_api.config import config
    c, name = live
    rows = [row(n, doi=f"https://doi.org/10.1234/{n}") for n in range(1, 4)]
    c.bulk(name, [engine.project("works", r) for r in rows])
    manifest = dict(engine="manticore", build="fixture", state="validated", collection="earth_scope_v1",
                    collection_signature="fixed", expected_groups={"01_core_journals": 3}, earth_expected=30,
                    ready_scopes=["earth_sciences"], indexes={"works": name}, incremental=True,
                    segments=[dict(index=name, entity="works", count=3, sealed=True)])
    meta = dict(generation="test_mc", _search_build=manifest)
    monkeypatch.setattr(api, "generation", lambda: meta)
    monkeypatch.setattr(manticore_api, "client", lambda: c)
    monkeypatch.setattr(manticore_api, "read_generation", lambda g: {"generation": g})
    monkeypatch.setattr(search_versions, "attach", lambda *a, **kw: meta)
    b = Mock()
    records = {r["id"]: json.loads(r["raw"]) for r in rows}
    b.details.side_effect = lambda g, e, keys: {k: records[k] for k in keys}
    monkeypatch.setattr(api, "backend", lambda: b)
    client = TestClient(api.app)
    headers = {"X-API-Key": config()["api_keys"][0]}
    params = dict(search="climate", search_scope="earth_sciences", per_page=2, sort="id:asc", count="exact")
    first = client.get("/v1/works", params=params, headers=headers)
    assert first.status_code == 200, first.text
    first = first.json()
    assert first["meta"]["count"] == 3 and not first["meta"]["earth_complete"]
    assert first["results"][0]["doi"] == "https://doi.org/10.1234/1"
    token = first["meta"]["next_cursor"]
    second = client.get("/v1/works", params=params | {"cursor": token}, headers=headers)
    assert second.status_code == 200, second.text
    second = second.json()
    assert second["meta"]["count"] == 3 and second["meta"]["next_cursor"] is None
    assert second["results"] == [records["W3"]]
    wrong = client.get("/v1/works", params=params | {"cursor": token, "search": "wrong"}, headers=headers)
    assert wrong.status_code == 422
    matched = client.post("/v1/references/match", headers=headers,
        json={"references": [{"title": "Climate change in the oceans"}]})
    assert matched.status_code == 200, matched.text
    assert matched.json()["results"][0]["candidates"][0]["work"]["doi"]
    b.os.assert_not_called()


def test_catalog_does_not_silently_apply_unindexed_work_filters():
    from openalex_api.manticore_api import listing
    with pytest.raises(ValueError, match="not indexed for this catalog"):
        listing("sources", {"query": {"term": {"is_oa": True}}}, None, 1, 20, None,
                "exact", False, "available", {})


def test_numeric_pruning_never_turns_or_or_not_into_required_filters():
    from openalex_api.manticore_api import search_request
    body = query("works", "", "all", "year:2020|2021,is_oa:!true")
    native = search_request("works", body, ["fixture"], 10)
    clauses = native["query"]["bool"]["must"]
    assert {"equals": {"is_oa": 1}} not in clauses
    assert clauses[1] == {"bool": {"should": [
        {"bool": {"must": [{"equals": {"publication_year": 2020}}]}},
        {"bool": {"must": [{"equals": {"publication_year": 2021}}]}}]}}
    body = query("works", "", "all", "year:2020..2021,is_oa:false")
    native = search_request("works", body, ["fixture"], 10)
    assert {"equals": {"is_oa": 0}} in native["query"]["bool"]["must"]
    assert {"range": {"publication_year": {"gte": 2020, "lte": 2021}}} in native["query"]["bool"]["must"]


def test_acceptance_positive_filter_cannot_pass_an_empty_response():
    from openalex_api.earth_delivery import validate_response
    import httpx
    with pytest.raises(AssertionError, match="too few"):
        validate_response({"expected": {"shape": "search", "min_results": 1}},
                          httpx.Response(200, json={"results": []}), {})


def test_readiness_uses_selected_engine_not_stopped_lucene(monkeypatch):
    from openalex_api import api, manage, manticore_api, manticore_build
    seen = []
    monkeypatch.setattr(manage, "health", lambda name: seen.append(name) or name != "opensearch")
    monkeypatch.setattr(manticore_api, "client", lambda: None)
    monkeypatch.setattr(manticore_build, "tables", lambda c: {"table1"})
    meta = dict(metadata_only=True, mode="full", _search_build={"engine": "manticore", "segments": [{"index": "table1"}]})
    assert api.availability(meta)["search_ready"]
    assert seen == ["clickhouse", "manticore"]
    monkeypatch.setattr(manticore_build, "tables", lambda c: set())
    assert not api.availability(meta)["search_ready"]


@pytest.mark.parametrize("lose_local_table", [False, True])
@pytest.mark.parametrize("hard_stop", [False, True])
def test_live_builder_recovers_unreceipted_write_and_lost_runtime(live, tmp_path, monkeypatch, lose_local_table, hard_stop):
    import gzip
    import re
    from openalex_api import manticore_build as build, official
    from openalex_api.config import atomic_json
    c, _ = live
    group = "01_core_journals"
    g = "test_mc_" + uuid.uuid4().hex[:8]
    path = tmp_path / "build"
    collection = tmp_path / "exports/earth_collections" / g / "earth_scope_v1"
    collection.mkdir(parents=True)
    rows = [row(n) for n in range(1, 4)]
    records = {r["id"]: r for r in rows}
    entries = [dict(openalex_id=r["id"], group=group, source_version=r["version"], raw_digest=r["digest"]) for r in rows]
    data = gzip.compress(b"\n".join(json.dumps(r).encode() for r in entries))
    (collection / "members.jsonl.gz").write_bytes(data)
    atomic_json(collection / "manifest.json", dict(signature="frozen", blocks=1))
    atomic_json(collection / "verification.json", dict(signature="frozen", passed=True, complete=True, counts={group: 3}))
    atomic_json(collection / "blocks/00000000.json", dict(sequence=0, files=[dict(group=group, path="members.jsonl.gz", count=3, sha256=hashlib.sha256(data).hexdigest())]))
    monkeypatch.setattr(build, "ROOT", tmp_path)
    (tmp_path / "var/engines").mkdir(parents=True)
    from openalex_api.config import ROOT
    (tmp_path / "var/engines/manticore-29.9.0").symlink_to(ROOT / "var/engines/manticore-29.9.0", target_is_directory=True)
    monkeypatch.setattr(build, "build_path", lambda *a: path)
    monkeypatch.setattr(build, "generation_path", lambda *a: tmp_path)
    monkeypatch.setattr(build, "signature", lambda *a: "projection")
    monkeypatch.setattr(build, "require_metadata", lambda g: {"signature": "metadata"})
    monkeypatch.setattr(build, "read_generation", lambda g: {"expected": {"topics": {"records": 0}, "sources": {"records": 0}}})
    monkeypatch.setattr(build, "BATCH", 1)
    monkeypatch.setattr(build, "MAX_BLOCK", 2)
    monkeypatch.setattr(official, "source_table", lambda *a: "fixture")
    class Metadata:
        def ch(self, query):
            return [records[k] for k in re.findall(r"'(W\d+)'", query)]
        def close(self):
            pass
    monkeypatch.setattr(build, "Backend", lambda **kw: Metadata())
    calls = 0
    class Interrupted(engine.Client):
        def bulk(self, table, documents):
            nonlocal calls
            result = super().bulk(table, documents)
            calls += 1
            if calls == 2:
                if hard_stop:
                    raise SystemExit("injected hard stop before manifest checkpoint")
                raise RuntimeError("injected after engine acknowledgment before receipt")
            return result
    url = os.environ["MANTICORE_TEST_URL"]
    monkeypatch.setattr(build, "Client", lambda **kw: Interrupted(url, **kw))
    try:
        with pytest.raises(SystemExit if hard_stop else RuntimeError, match="injected"):
            build.build(g, "build1", [group])
        state = json.loads((path / "build.json").read_text())
        assert state["segments"][0]["count"] == (0 if hard_stop else 1)
        if lose_local_table:
            c.sql("DROP TABLE " + state["segments"][0]["index"])
        result = build.build(g, "build1", [group])
        assert result["state"] == "sealed" and result["entities"]["works"]["count"] == 3
        assert [s["count"] for s in result["segments"]] == [2, 1]
        assert all(s["sealed"] and s["restore_verified"] for s in result["segments"])
    finally:
        if (path / "build.json").exists():
            for s in json.loads((path / "build.json").read_text())["segments"]:
                if s["index"] in build.tables(c):
                    c.sql("DROP TABLE " + s["index"])


@pytest.mark.parametrize('fail_packet', [False, True])
def test_live_parallel_batch_receipt_is_atomic_and_replay_safe(live, tmp_path, fail_packet):
    import threading
    from openalex_api.manticore_build import write_batch, verify_segment
    c, name = live
    segment = dict(index=name, entity='works', count=0, batches=[])
    rows = [row(n) for n in range(1, 17)]
    barrier = threading.Barrier(4)
    seen = []
    class Concurrent:
        def bulk(self, table, docs):
            seen.append(docs[0][0])
            barrier.wait(timeout=10)
            result = c.bulk(table, docs)
            if fail_packet and docs[0][0] == 1:
                raise RuntimeError('injected after one packet acknowledgment')
            return result
        def rows(self, sql):
            assert len(seen) == 4
            return c.rows(sql)
    if fail_packet:
        with pytest.raises(RuntimeError, match='injected'):
            write_batch(Concurrent(), None, tmp_path, segment, rows, bulk_workers=4)
        assert segment['count'] == 0 and segment['batches'] == []
        assert not list(tmp_path.rglob('*.json.gz'))
        write_batch(c, None, tmp_path, segment, rows, bulk_workers=4)
    else:
        timings = write_batch(Concurrent(), None, tmp_path, segment, rows, bulk_workers=4)
        assert all(t >= 0 for t in timings.values())
    assert len(seen) == 4 and segment['count'] == 16 and len(segment['batches']) == 1
    verify_segment(c, tmp_path, segment)
