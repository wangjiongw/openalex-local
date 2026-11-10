import copy
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from openalex_api import api, search_versions as versions, search_projection, proofs
from openalex_api.backend import index
from openalex_api.config import config
from openalex_api.query import query, decode_cursor
from openalex_api.official_text import indexed_predicate, build, can_route


def test_search_projection_digest_and_metadata_contract_are_independent(monkeypatch):
    record = dict(id="W1", title="C++: A, B", authorships=[dict(author=dict(id="A1", display_name="甲"))])
    original = copy.deepcopy(record)
    projected = search_projection.project("works", record)
    assert record == original and projected["name_literal"] == "c++: a, b"
    assert len(projected["projection_hash"]) == 64
    assert projected["projection_hash"] != search_projection.project("works", record | dict(title="different"))["projection_hash"]
    real = proofs.file_hash
    monkeypatch.setattr(proofs, "read_generation", lambda g: {"manifest_sha256": "manifest"})
    before = proofs.semantic("example")
    signature = versions.signature()
    monkeypatch.setattr(proofs, "file_hash", lambda p: "changed" if p.name == "search_projection.py" else real(p))
    assert proofs.semantic("example") == before
    assert versions.signature() != signature


def test_author_variants_preserve_source_and_non_latin_names():
    source = dict(id="W1", title="one", authorships=[dict(author=dict(id="A1", display_name="Martin Jínek"))])
    projected = search_projection.project("works", source)
    assert projected["authorships"][0]["author_name"] == ["Martin Jínek", "Martin Jinek"]
    assert source["authorships"][0]["author"]["display_name"] == "Martin Jínek"
    assert search_projection.latin_alias("张三 ガ") == "张三 ガ"
    author = search_projection.project("authors", dict(id="A1", display_name="José", display_name_alternatives=["Pepe"]))
    assert author["aliases"] == ["Pepe", "Jose"]


def test_short_successful_load_is_not_formal_acceptance():
    from openalex_api.search_acceptance import require_passed
    with pytest.raises(RuntimeError, match="incomplete"):
        require_passed(dict(passed=True, generation="g", build="b"), "g", "b")


def test_qualified_engine_uses_fresh_shared_data_without_touching_legacy(tmp_path, monkeypatch):
    from openalex_api import manage, search_storage
    monkeypatch.setattr(manage, "ROOT", tmp_path)
    monkeypatch.setattr(manage, "config", lambda: config() | dict(ch_admin_password="fixture", ch_reader_password="fixture"))
    monkeypatch.setattr(search_storage, "qualified_options", lambda g: ["qualified-observer"])
    legacy = tmp_path / "var/data/opensearch"
    legacy.mkdir(parents=True)
    (legacy / "retained-evidence").write_text("keep")
    (tmp_path / "config").mkdir()
    (tmp_path / "var/active.json").write_text('{"generation":"test_version"}')
    manage.engine_configs()
    body = (tmp_path / "config/opensearch/opensearch.yml").read_text()
    assert "path.data: " + str(tmp_path / "var/data/search-v2") in body
    assert "node.store.allow_mmap: false" in body
    assert (legacy / "retained-evidence").read_text() == "keep"


def test_combination_keeps_names_and_affiliations_on_one_authorship():
    value = query("works", "climate", "title", "institution:I1,author:A1", author_search='"John Smith"')
    nested = value["query"]["bool"]["filter"][0]["nested"]["query"]["bool"]["must"]
    assert len(nested) == 3
    assert nested[2]["multi_match"]["query"] == "John Smith"
    assert query("works", author_search="甲")["sort"][0] == {"_score": {"order": "desc"}}
    for entity, field in (("authors", "title"), ("works", "author")):
        with pytest.raises(ValueError):
            query(entity, "test", field, author_search="name")


def test_indexed_filter_null_or_intersection_and_literal_punctuation():
    null_or = indexed_predicate("publication_year", "int", "null|2020")
    assert len(null_or["bool"]["should"]) == 2
    assert indexed_predicate("name_literal", "str", '"C++|A"') == {"term": {"name_literal": "c++|a"}}
    both = indexed_predicate("authorships.institution_id", "id", "I1+I2")
    assert len(both["bool"]["must"]) == 2
    assert all("nested" in clause for clause in both["bool"]["must"])
    mixed = indexed_predicate("authorships.author_id", "id", "A1|A2+A3")
    assert len(mixed["bool"]["must"]) == 2
    assert len(mixed["bool"]["must"][0]["bool"]["should"]) == 2
    for value in ("nan", "inf", "-inf"):
        with pytest.raises(ValueError, match="finite"):
            indexed_predicate("fwci", "float", value)
    missing = indexed_predicate("authorships.author_id", "id", "null")
    assert missing["bool"]["must_not"][0]["nested"]["query"] == {"exists": {"field": "authorships.author_id"}}
    assert can_route("works", {}, [("authorships.author.id", "A1")])
    assert not can_route("works", {}, [("cites", "W1")])
    assert not can_route("authors", {}, [("publication_year", "2020")])
    value = build("works", {}, [("type", "book")])
    assert value["sort"][0] == {"id": {"order": "asc"}}


@pytest.fixture
def manifests(tmp_path, monkeypatch):
    monkeypatch.setattr(versions, "generation_path", lambda g: tmp_path / g)
    monkeypatch.setattr(versions, "signature", lambda: "projection")
    for name in ("first", "second"):
        manifest = dict(generation="test_version", build=name, state="validated", projection_signature="projection",
                        indexes={"works": "works-" + name})
        versions.save(manifest)
    yield tmp_path


def activate(root, build):
    (root / "test_version/search-active.json").write_text(json.dumps(dict(build=build)))


def test_pinned_index_survives_switch_and_disabled_search_keeps_metadata(manifests):
    activate(manifests, "first")
    with versions.pinned("test_version", versions.active("test_version")):
        activate(manifests, "second")
        assert index("test_version", "works") == "works-first"
    with versions.pinned("another_generation", None):
        assert index("test_version", "works") == "works-second"
    activate(manifests, None)
    meta = versions.attach(dict(generation="test_version", metadata_only=True))
    assert meta["metadata_only"] and meta["_search_disabled"]
    with pytest.raises(Exception) as raised:
        api.require_search(meta)
    assert raised.value.status_code == 503


def test_corrupt_manifest_does_not_break_metadata_attachment(manifests):
    activate(manifests, "first")
    p = versions.build_path("test_version", "first") / "build.json"
    p.write_text("not json")
    meta = versions.attach(dict(generation="test_version", metadata_only=True))
    assert meta["metadata_only"] and meta["_search_error"] and not meta["_search_build"]
    with pytest.raises(ValueError):
        versions.build_path("test_version", "../wrong")


def test_cursor_binds_original_search_build_after_publication(manifests, monkeypatch):
    activate(manifests, "first")
    metadata = dict(generation="test_version", metadata_only=True)
    monkeypatch.setattr(api, "generation", lambda: versions.attach(metadata))
    monkeypatch.setattr(api, "read_generation", lambda g: metadata.copy())
    backend = Mock()
    backend.os.side_effect = [dict(pit_id="old-pit"),
        dict(hits=dict(total=dict(value=2, relation="eq"), hits=[dict(_id="W1", sort=["W1"]), dict(_id="W2", sort=["W2"])])),
        dict(hits=dict(total=dict(value=2, relation="eq"), hits=[dict(_id="W2", sort=["W2"])])), {}]
    backend.details.side_effect = lambda g, e, keys: {k: dict(id=k, title="climate") for k in keys}
    monkeypatch.setattr(api, "backend", lambda: backend)
    with TestClient(api.app, headers={"X-API-Key": config()["api_keys"][0]}) as client:
        response = client.get("/v1/works", params=dict(search="climate", per_page=1))
        assert response.status_code == 200, response.text
        cursor = response.json()["meta"]["next_cursor"]
        assert decode_cursor(cursor, config()["cursor_secret"])["search_build"] == "first"
        activate(manifests, "second")
        response = client.get("/v1/works", params=dict(search="climate", per_page=1, cursor=cursor))
        assert response.status_code == 200, response.text
        assert response.json()["meta"]["search_build"] == "first"


def test_projection_verification_rejects_wrong_content_with_matching_version():
    row = dict(id="W1", version=4, raw='{"id":"W1","title":"one"}')
    doc = dict(_id="W1", _version=4, found=True, fields={"projection_hash": ["wrong"]})
    with pytest.raises(RuntimeError, match="projection mismatch"):
        versions.checked_documents([row], [doc], "works")
    doc["fields"]["projection_hash"] = [search_projection.project("works", json.loads(row["raw"]))["projection_hash"]]
    versions.checked_documents([row], [doc], "works")


def test_incomplete_qualification_cannot_release_build(tmp_path, monkeypatch):
    from openalex_api import search_storage as storage
    monkeypatch.setattr(storage, "generation_path", lambda g: tmp_path)
    monkeypatch.setattr(storage, "dependencies", lambda: {})
    (tmp_path / "search-qualification.json").write_text(json.dumps(dict(passed=True, dependencies={}, count=20_000_000,
        active_seconds=86399, updates=10000, crash_recovered=True, forward_reverse=True, physical={"0": True})))
    with pytest.raises(RuntimeError, match="incomplete"):
        storage.require_qualification("example")


def test_failed_acknowledgment_does_not_advance_qualification_cursor(tmp_path, monkeypatch):
    from openalex_api import search_storage as storage
    monkeypatch.setattr(storage, "ROOT", tmp_path)
    monkeypatch.setattr(storage, "generation_path", lambda g: tmp_path / "generation")
    q = storage.Qualification("test_version", "attempt")
    q.s = dict(phase="writing", count=0, updates=0, active_seconds=0, after="")
    q.guard = Mock()
    q.b = Mock()
    q.b.cfg = {"os_url": "http://unused"}
    q.b.http.head.return_value.status_code = 404
    q.rows = Mock(return_value=[dict(id="W1", version=1, raw='{"id":"W1"}')])
    q.b.bulk.side_effect = RuntimeError("injected bulk error")
    with pytest.raises(RuntimeError, match="injected"):
        q.workload()
    assert q.s["after"] == "" and q.s["count"] == 0
