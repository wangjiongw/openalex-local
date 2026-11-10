"""Real ClickHouse/OpenSearch end-to-end checks; opt in with OA_LIVE_TESTS=1."""

import gzip, json, os, time
import pytest
from fastapi.testclient import TestClient
from openalex_api import ingest, api
from openalex_api.config import ENTITIES, ROOT, config
from openalex_api.backend import Backend, database, index

pytestmark = pytest.mark.skipif(
    os.getenv("OA_LIVE_TESTS") != "1", reason="requires running real backends"
)


@pytest.fixture(scope="module")
def dataset():
    g = "test_" + str(int(time.time()))
    root = ROOT / "var/test-fixtures" / g
    cfg = config()
    records = {}
    for e in ENTITIES:
        records[e] = [
            {
                "id": "https://openalex.org/" + e + "/example",
                "display_name": "Example " + e,
                "new_field": {"preserved": None},
                "cited_by_count": None,
            }
        ]
    records["works"] = [
        {
            "id": "https://openalex.org/W1",
            "title": "气候变化与水循环",
            "doi": "https://doi.org/10.1000/A.(B)/C",
            "publication_year": 2022,
            "is_xpac": False,
            "cited_by_count": 999,
            "abstract_inverted_index": {"气候": [0], "变化": [1], "研究": [2]},
            "referenced_works": [
                "https://openalex.org/W2",
                "https://openalex.org/W2",
                "https://openalex.org/W999",
            ],
            "authorships": [
                {
                    "author": {"id": "https://openalex.org/A1", "display_name": "甲"},
                    "institutions": [
                        {"id": "https://openalex.org/I1", "display_name": "一大学"}
                    ],
                },
                {
                    "author": {"id": "https://openalex.org/A2", "display_name": "乙"},
                    "institutions": [
                        {"id": "https://openalex.org/I2", "display_name": "二大学"}
                    ],
                },
            ],
            "future_field": {"not_lost": True},
        },
        {
            "id": "https://openalex.org/W2",
            "title": "Climate change and water cycle",
            "doi": "10.1000/collision",
            "is_xpac": True,
            "cited_by_count": None,
        },
        {
            "id": "https://openalex.org/W3",
            "title": "Climate change and water cycle",
            "doi": "10.1000/collision",
            "is_xpac": None,
            "cited_by_count": 10,
        },
    ]
    records["authors"] = [
        {
            "id": "https://openalex.org/A1",
            "display_name": "甲",
            "display_name_alternatives": ["First Author"],
            "last_known_institutions": [
                {"id": "I2", "display_name": "Current institution"}
            ],
        }
    ]
    records["institutions"] = [
        {
            "id": "https://openalex.org/I1",
            "display_name": "一大学",
            "country_code": "CN",
        }
    ]
    for e, rs in records.items():
        path = root / "data" / e / "updated_date=2026-01-01/part_0000.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt") as f:
            for r in rs:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        manifest = {
            "entries": [
                {
                    "url": f"s3://openalex/data/{e}/updated_date=2026-01-01/part_0000.gz",
                    "meta": {
                        "content_length": path.stat().st_size,
                        "record_count": len(rs),
                    },
                }
            ]
        }
        (root / "data" / e / "manifest").write_text(json.dumps(manifest))
    original = ingest.config
    ingest.config = lambda: cfg | {"snapshot": str(root), "workers": 2}
    try:
        ingest.create_generation(g, "full")
        ingest.run_import(g)
        # Simulate loss of checkpoint after sink acknowledgements: replay entire works shard.
        with ingest.connect(g) as db:
            db.execute(
                "UPDATE shards SET state='pending',line=0,records=0 WHERE entity='works'"
            )
        ingest.run_import(g)
        report = ingest.validate(g)
        assert report["entities"]["works"]["unique_entities"] == 3
        assert report["edges"]["references_out"] == 2
        yield g, records, report
    finally:
        ingest.config = original


@pytest.fixture
def client(dataset, monkeypatch):
    g, records, report = dataset
    monkeypatch.setattr(api, "generation", lambda: ingest.read_generation(g))
    monkeypatch.setattr(
        api, "config", lambda: config() | {"qps": 10000, "burst": 10000}
    )
    api.limiter.clear()
    with TestClient(api.app, headers={"X-API-Key": config()["api_keys"][0]}) as c:
        yield c


def test_lossless_all_entities(dataset):
    g, records, _ = dataset
    b = Backend()
    try:
        for e, rs in records.items():
            found = b.details(
                g, e, [r["id"].removeprefix("https://openalex.org/") for r in rs]
            )
            assert list(found.values()) == rs
    finally:
        b.close()


def test_search_and_nested_filter(client):
    r = client.get("/v1/works", params={"search": "气候变化", "filter": "year:2022"})
    assert r.status_code == 200, r.text
    assert r.json()["meta"]["count"] == 1
    r = client.get("/v1/works", params={"filter": "author:A1,institution:I2"})
    assert r.status_code == 200, r.text
    assert r.json()["meta"]["count"] == 0
    r = client.get("/v1/works", params={"filter": "author:A1,institution:I1"})
    assert r.json()["meta"]["count"] == 1
    assert (
        client.get("/v1/works", params={"corpus": "core"}).json()["meta"]["count"] == 1
    )


def test_title_author_combination_preserves_affiliation(client):
    params = dict(search="气候变化", search_field="title", author_search="甲", filter="year:2022,institution:I1")
    reply = client.get("/v1/works", params=params)
    assert reply.status_code == 200, reply.text
    assert reply.json()["meta"]["count"] == 1
    reply = client.get("/v1/works", params=params | dict(filter="institution:I2"))
    assert reply.status_code == 200 and reply.json()["meta"]["count"] == 0
    assert client.get("/v1/authors", params=dict(author_search="甲")).status_code == 422


def test_versioned_projection_roundtrip_and_common_filter_routing(dataset, monkeypatch):
    from openalex_api import search_versions as versions, search_projection
    from openalex_api.config import atomic_json, read_generation
    from openalex_api.backend import sql_string
    g, records, _ = dataset
    manifest = dict(generation=g, build="integration", state="validated", projection_signature=versions.signature(),
                    indexes={e: "oa-version-test-" + g.replace("_", "-") + "-" + e for e in ENTITIES})
    b = Backend(writer=True)
    versions.save(manifest)
    try:
        with versions.pinned(g, manifest):
            for entity in ENTITIES:
                b.os("PUT", "/" + manifest["indexes"][entity], search_projection.mapping())
                rows = b.ch(f"SELECT id,version,raw FROM {database(g)}.entities FINAL WHERE entity={sql_string(entity)} ORDER BY id FORMAT JSONEachRow")
                b.bulk(g, entity, [(r["id"], int(r["version"]), search_projection.project(entity, json.loads(r["raw"]))) for r in rows])
                versions.checked_documents(rows, versions.mget(b, manifest["indexes"][entity], [r["id"] for r in rows]), entity)
                b.os("POST", "/" + manifest["indexes"][entity] + "/_refresh")
            assert versions.verify(b, g, manifest, "works") == 3
        monkeypatch.setattr(api, "generation", lambda: versions.attach(read_generation(g), "integration"))
        monkeypatch.setattr(api, "backend", lambda: b)
        with TestClient(api.app, headers={"X-API-Key": config()["api_keys"][0]}) as c:
            reply = c.get("/works", params={"filter": "authorships.author.id:A1", "select": "doi,title,publication_year"})
            assert reply.status_code == 200, reply.text
            assert reply.json()["meta"]["search_build"] == "integration" and reply.json()["meta"]["count"] == 1
            assert "projection_hash" not in reply.json()["results"][0]
            reply = c.get("/works", params={"filter": "publication_year:null|2022"})
            assert reply.status_code == 200 and reply.json()["meta"]["count"] == 3
            reply = c.get("/v1/works", params=dict(search="气候变化", search_field="title", author_search="甲", filter="institution:I1"))
            assert reply.status_code == 200 and reply.json()["meta"]["count"] == 1
    finally:
        for name in manifest["indexes"].values():
            b.http.delete(b.cfg["os_url"] + "/" + name)
        b.close()


def test_author_year_help_recall_before_fifty_candidate_cutoff(monkeypatch):
    import uuid
    from openalex_api import search_projection, search_versions as versions
    from openalex_api.query import query
    g = "test_biblio_" + uuid.uuid4().hex[:12]
    manifest = dict(generation=g, build="candidate", state="validated", projection_signature=versions.signature(),
                    indexes={"works": "oa-biblio-" + g.replace("_", "-")})
    versions.save(manifest)
    records = [dict(id="https://openalex.org/W" + str(n), title="Deep learning", publication_year=2000) for n in range(100, 155)]
    records.append(dict(id="https://openalex.org/W999", title="Deep learning", publication_year=2015,
                        doi="https://doi.org/10.1000/target", authorships=[dict(author=dict(id="A1", display_name="Yann LeCun"))]))
    b = Backend(writer=True)
    try:
        b.create(g, {}, search=False)
        b.insert(g, "entities", [dict(entity="works", id=r["id"].split("/")[-1], version=1,
                                      publication_year=r["publication_year"], raw=json.dumps(r)) for r in records])
        with versions.pinned(g, manifest):
            b.os("PUT", "/" + index(g, "works"), search_projection.mapping())
            b.bulk(g, "works", [(r["id"].split("/")[-1], 1, search_projection.project("works", r)) for r in records])
            b.os("POST", "/" + index(g, "works") + "/_refresh")
            plain = query("works", "Deep learning", "title") | dict(size=50)
            hits = b.os("POST", "/" + index(g, "works") + "/_search", plain)["hits"]["hits"]
            assert "W999" not in [h["_id"] for h in hits]
        monkeypatch.setattr(api, "generation", lambda: versions.attach(dict(generation=g, metadata_only=True), "candidate"))
        monkeypatch.setattr(api, "backend", lambda: b)
        with TestClient(api.app, headers={"X-API-Key": config()["api_keys"][0]}) as c:
            response = c.post("/v1/references/match", json={"references": [dict(title="Deep learning", author="Yann LeCun", year=2015)]})
            assert response.status_code == 200, response.text
            match = response.json()["results"][0]
            assert match["status"] == "candidates" and match["candidates"][0]["work"]["id"].endswith("/W999")
    finally:
        b.http.delete(b.cfg["os_url"] + "/" + manifest["indexes"]["works"])
        b.ch("DROP DATABASE IF EXISTS " + database(g))
        b.close()


def test_builder_snapshot_restore_and_rejection_of_unqualified_acceptance(dataset, monkeypatch):
    from openalex_api import search_versions as versions, search_storage, search_acceptance
    from openalex_api.config import read_generation
    import httpx
    g, _, _ = dataset
    manifest = dict(generation=g, build="builder", state="building", metadata_signature="fixture-metadata",
                    projection_signature=versions.signature(), shards={e: 1 for e in ENTITIES},
                    indexes={e: "oa-builder-test-" + g.replace("_", "-") + "-" + e for e in ENTITIES},
                    entities={e: dict(after="", count=0, done=False) for e in ENTITIES})
    versions.save(manifest)
    monkeypatch.setattr(versions, "require_metadata", lambda g: {"signature": "fixture-metadata"})
    monkeypatch.setattr(search_storage, "require_qualification", lambda g: {"path": "fixture-only"})
    monkeypatch.setattr(search_storage, "check_runtime", lambda b: {})
    # Production acceptance limits stay intact; this test deliberately reaches and fails that gate.
    def incomplete_acceptance(g, build, url):
        with httpx.Client(base_url=url, headers={"X-API-Key": config()["api_keys"][0]}, timeout=20, trust_env=False) as h:
            response = h.get("/v1/works", params=dict(search="气候变化", search_field="title", author_search="甲"))
            assert response.status_code == 200, response.text
            assert response.json()["meta"]["count"] == 1
        return dict(passed=True, generation=g, build=build, fixture_only=True)
    monkeypatch.setattr(search_acceptance, "run", incomplete_acceptance)
    b = Backend(writer=True)
    try:
        sealed = versions.build(g, "builder")
        assert sealed["state"] == "sealed" and sealed["entities"]["works"]["count"] == 3
        with pytest.raises(RuntimeError, match="acceptance is incomplete"):
            versions.validate(g, "builder")
        state = versions.read(g, "builder")
        assert state["state"] == "sealed" and state["physical_checked"] and state["restore_checked"]
        assert (versions.build_path(g, "builder") / "restore-check.json").exists()
        assert versions.active(g) is None
    finally:
        for name in manifest["indexes"].values():
            b.http.delete(b.cfg["os_url"] + "/" + name)
        b.close()


def test_official_boolean_search_and_paging(client):
    response = client.get("/works", params={"search": '"Climate change" OR 气候', "select": "id,title,doi,relevance_score", "per_page": 1, "cursor": "*"})
    seen = []
    while True:
        assert response.status_code == 200, response.text
        data = response.json()
        assert data["meta"]["count"] == 3
        seen.extend(r["id"] for r in data["results"])
        assert all("relevance_score" in r for r in data["results"])
        cursor = data["meta"]["next_cursor"]
        if not cursor:
            break
        response = client.get("/works", params={"search": '"Climate change" OR 气候', "select": "id,title,doi,relevance_score", "per_page": 1, "cursor": cursor})
    assert len(seen) == len(set(seen)) == 3
    response = client.get("/works", params={"search": "气候", "filter": "authorships.author.id:A1,authorships.institutions.id:I2"})
    assert response.status_code == 200, response.text
    assert response.json()["meta"]["count"] == 1
    response = client.get("/works", params={"search": "climate NOT water"})
    assert response.status_code == 200 and response.json()["meta"]["count"] == 0


def test_exact_doi_and_ambiguity(client):
    r = client.get("/v1/works/lookup", params={"identifier": "10.1000/A.(B)/C"})
    assert r.status_code == 200, r.text
    assert r.json()["id"].endswith("/W1")
    assert client.get("/v1/works/10.1000/A.(B)/C").status_code == 200
    assert (
        client.get(
            "/v1/works/lookup", params={"identifier": "10.1000/collision"}
        ).status_code
        == 409
    )
    assert client.get("/v1/keywords/example").status_code == 200


def test_cursor_and_selection(client):
    params = {"per_page": 1, "select": "id,title"}
    seen = []
    for i in range(4):
        r = client.get("/v1/works", params=params)
        assert r.status_code == 200, r.text
        v = r.json()
        seen.extend(x["id"] for x in v["results"])
        token = v["meta"]["next_cursor"]
        if not token:
            break
        assert (
            client.get(
                "/v1/works", params=params | {"cursor": token, "filter": "year:2020"}
            ).status_code
            == 422
        )
        params["cursor"] = token
    assert len(seen) == 3 and len(set(seen)) == 3


def test_edges_and_counts(client):
    v = client.get("/v1/works/W1/references", params={"per_page": 1}).json()
    assert v["results"][0]["resolved"] is True
    r = client.get(
        "/v1/works/W1/references",
        params={"per_page": 1, "cursor": v["meta"]["next_cursor"]},
    )
    assert r.status_code == 200, r.text
    assert r.json()["results"][0]["resolved"] is False
    r = client.get("/v1/works/W1/citation-counts")
    assert r.status_code == 200, r.text
    assert (
        r.json()["cited_by_count"] == 999
        and r.json()["unique_reference_edges"] == 2
        and r.json()["raw_reference_array_length"] == 3
    )
    assert (
        client.get("/v1/works/W2/citations").json()["results"][0]["id"].endswith("/W1")
    )


def test_batch_match_and_errors(client):
    r = client.post(
        "/v1/works/batch", json={"identifiers": ["W1", "W999", "10.1000/collision"]}
    )
    assert r.status_code == 200, r.text
    assert [x["status"] for x in r.json()["results"]] == [
        "found",
        "not_found",
        "ambiguous",
    ]
    r = client.post(
        "/v1/references/match",
        json={
            "references": [
                {"doi": "10.1000/A.(B)/C"},
                {"title": "Climate change and water cycle"},
            ]
        },
    )
    assert r.status_code == 200, r.text
    assert [x["status"] for x in r.json()["results"]] == ["matched", "candidates"]
    assert client.get("/v1/works", params={"bogus": "x"}).status_code == 422
    assert client.get("/v1/works", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/v1/works", params={"per_page": 101}).status_code == 422
    assert client.get("/v1/works", params={"group_by": "type"}).status_code == 200


def test_old_cursor_survives_active_pointer_change(client, monkeypatch):
    params = {"per_page": 1, "select": "id"}
    first = client.get("/v1/works", params=params).json()

    def changed():
        raise AssertionError("cursor must not consult the newly active generation")

    monkeypatch.setattr(api, "generation", changed)
    second = client.get(
        "/v1/works", params=params | {"cursor": first["meta"]["next_cursor"]}
    )
    assert second.status_code == 200, second.text
    assert second.json()["results"][0]["id"] != first["results"][0]["id"]


def test_equal_title_with_punctuation_and_missing_author(client):
    r = client.get(
        "/v1/works", params={"title_equal": "Climate change, and water cycle"}
    )
    assert r.status_code == 200, r.text
    assert r.json()["meta"]["count"] == 2
    r = client.get("/v1/works", params={"filter": "author:null"})
    assert r.json()["meta"]["count"] == 2


def test_reader_is_readonly_and_index_is_sealed(dataset):
    from openalex_api.backend import BackendError

    g, _, _ = dataset
    b = Backend()
    try:
        with pytest.raises(BackendError, match="READONLY"):
            b.ch(f"CREATE TABLE {database(g)}.readonly_probe (x UInt8) ENGINE=Memory")
    finally:
        b.close()
    b = Backend(writer=True)
    try:
        with pytest.raises(BackendError):
            b.os(
                "PUT",
                "/" + index(g, "works") + "/_doc/W4",
                {"id": "W4", "name": "must not be written"},
            )
    finally:
        b.close()
