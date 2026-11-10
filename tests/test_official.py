import json
import os
import uuid

import pytest
from fastapi.testclient import TestClient

from openalex_api import api, official
from openalex_api.backend import Backend, database
from openalex_api.config import atomic_json, config, generation_path
from openalex_api.model import external_ids
from openalex_api.official_text import text_query, build


def test_filter_quotes_ranges_and_pasted_titles():
    assert official.split_filters('doi:"10.1234/a,b",publication_year:2020-2025') == [
        ("doi", '"10.1234/a,b"'), ("publication_year", "2020-2025")]
    assert official.split_filters("title.search:Music, 1785,type:book") == [
        ("title.search", "Music, 1785"), ("type", "book")]
    with pytest.raises(ValueError):
        official.split_filters('doi:"unfinished')
    for value in ("2026-99-01", "no date"):
        with pytest.raises(ValueError):
            official.predicate("works", "publication_date", value)
    q = official.predicate("works", "publication_year", "2020-2025")
    assert "publication_year>=2020" in q and "publication_year<=2025" in q


def test_query_values_never_become_sql():
    q = official.where_sql("test_queries", "works", [("title", "x' OR 1=1 --")])
    assert "x\\' or 1=1 --" in q
    with pytest.raises(ValueError):
        official.predicate("works", "title); DROP TABLE entities --", "x")
    with pytest.raises(ValueError):
        official.where_sql("test_queries", "works", [("cites", "W1) OR 1=1")])
    with pytest.raises(ValueError):
        official.predicate("works", "fwci", "nan")


@pytest.mark.parametrize("entity,value,expected", [
    ("works", "doi:10.1000/AbC.(1)/x", ("doi", "10.1000/abc.(1)/x")),
    ("works", "https://pubmed.ncbi.nlm.nih.gov/12345/", ("pmid", "12345")),
    ("authors", "https://orcid.org/0000-0001-2345-6789", ("orcid", "0000-0001-2345-6789")),
    ("institutions", "ror:012xyz", ("ror", "012xyz")),
    ("sources", "issn_l:1234-5678", ("issn", "1234-5678")),
    ("concepts", "wikidata:Q123", ("wikidata", "https://www.wikidata.org/wiki/q123")),
    ("works", "https://openalex.org/w123", ("openalex", "W123")),
    ("countries", "US", ("openalex", "countries/US")),
])
def test_external_identifiers(entity, value, expected):
    assert official.identifier(entity, value) == expected


def test_keyset_handles_nulls_ties_and_directions():
    sorts = official.sort_fields("works", "publication_year:desc,id:asc")
    q = official.seek_sql(sorts, [2020, "W1"])
    assert "publication_year<2020" in q and "isNull(publication_year)" in q
    assert "publication_year=2020 AND (id>'W1'" in q
    assert "isNull(publication_year)" in official.seek_sql(sorts, [None, "W2"])


def test_boolean_search_is_parsed_without_query_string_field_injection():
    q = text_query('(climate OR weather) AND "water cycle" NOT pollution', ("name", "abstract"))
    assert len(q["bool"]["must"]) == 3
    assert q["bool"]["must"][0]["bool"]["minimum_should_match"] == 1
    assert q["bool"]["must"][1]["multi_match"]["type"] == "phrase"
    assert q["bool"]["must"][2]["bool"]["must_not"]
    assert text_query("title:secret", ("name",))["multi_match"]["query"] == "title:secret"
    for bad in ('"unterminated', '(a OR b', 'a AND', 'a OR )', '((a)', '(' * 40 + 'a' + ')' * 40):
        with pytest.raises(ValueError):
            text_query(bad, ("name",))
    q = build("works", {"search": "climate OR weather"}, [("authorships.author.id", "A1"), ("institutions.id", "I2")])
    assert len(q["query"]["bool"]["filter"]) == 2
    assert all("nested" in item for item in q["query"]["bool"]["filter"])


def test_final_is_removed_only_for_audited_frozen_unique_rows(tmp_path, monkeypatch):
    from unittest.mock import Mock
    from openalex_api import recovery
    meta = dict(generation="test_frozen", metadata_frozen=True)
    monkeypatch.setattr(official, "generation_path", lambda g: tmp_path)
    atomic_json(tmp_path / "metadata-release.json", dict(passed=True, metadata_signature="sig", entities={"works": {"unique_entities": 3}}))
    proof = Mock(return_value={"signature": "sig"})
    monkeypatch.setattr(recovery, "require_evidence", proof)
    b = Mock()
    b.ch.return_value = [{"n": 3}]
    assert official.source_table(b, meta, "works") == "oa_test_frozen.entities"
    b.ch.return_value = [{"n": 4}]
    assert official.source_table(b, meta, "works").endswith(" FINAL")
    proof.side_effect = RuntimeError("stale proof")
    assert official.source_table(b, meta, "works").endswith(" FINAL")
    assert official.source_table(b, meta | {"metadata_frozen": False}, "works").endswith(" FINAL")


@pytest.fixture(scope="module")
def stored():
    if os.getenv("OA_LIVE_TESTS") != "1":
        pytest.skip("requires running real ClickHouse")
    g = "test_official_" + uuid.uuid4().hex[:12]
    path = generation_path(g)
    path.mkdir(parents=True)
    meta = dict(generation=g, mode="full", metadata_only=True, metadata_frozen=False, complete=False)
    atomic_json(path / "generation.json", meta)
    works = [
        dict(id="https://openalex.org/W1", title="Alpha, beta", doi="https://doi.org/10.1234/a,b",
             publication_year=2020, publication_date="2020-06-01", cited_by_count=10,
             open_access={"is_oa": True}, related_works=["https://openalex.org/W3"],
             authorships=[{"author": {"id": "https://openalex.org/A1"}, "institutions": [{"id": "https://openalex.org/I1", "country_code": "US"}]},
                          {"author": {"id": "https://openalex.org/A2"}, "institutions": [{"id": "https://openalex.org/I2", "country_code": "GB"}]}],
             future_field={"preserved": True}),
        dict(id="https://openalex.org/W2", title="Gamma", doi="https://doi.org/10.1234/shared", publication_year=2021, cited_by_count=20),
        dict(id="https://openalex.org/W3", title="Delta", doi="https://doi.org/10.1234/shared", publication_year=None, cited_by_count=20),
    ]
    b = Backend(writer=True)
    b.create(g, {}, search=False)
    try:
        for e, records in [("works", works), ("authors", [dict(id="https://openalex.org/A1", display_name="Author A", orcid="https://orcid.org/0000-0001-2345-6789", works_count=3)]),
                           ("institutions", [dict(id="https://openalex.org/I1", display_name="Institution A", country_code="US", type="education")])]:
            b.insert(g, "entities", [dict(entity=e, id=r["id"].split("/")[-1], version=1,
                raw=json.dumps(r), publication_year=r.get("publication_year"), cited_by_count=r.get("cited_by_count")) for r in records])
            b.insert(g, "identifiers", [dict(entity=e, id=r["id"].split("/")[-1], scheme=s, value=v, version=1)
                for r in records for s, v in external_ids(r)])
        for direction in ("in", "out"):
            b.insert(g, "references_" + direction, [dict(citing=1, cited=2, version=1), dict(citing=2, cited=3, version=1)])
        yield meta
    finally:
        b.ch("DROP DATABASE " + database(g))
        b.close()


@pytest.fixture
def client(stored, monkeypatch):
    reader = Backend()
    monkeypatch.setattr(api, "generation", lambda: stored.copy())
    monkeypatch.setattr(api, "backend", lambda: reader)
    monkeypatch.setattr(api, "config", lambda: config() | dict(qps=0, concurrency=0))
    with TestClient(api.app, headers={"X-API-Key": config()["api_keys"][0]}) as c:
        yield c
    reader.close()


@pytest.mark.parametrize("filters,ids", [
    ("publication_year:2020-2021", ["W1", "W2"]),
    ("publication_year:!null", ["W1", "W2"]),
    ("publication_year:null", ["W3"]),
    ("publication_year:!2020", ["W2", "W3"]),
    ("from_publication_date:2020-01-01,to_publication_date:2020-12-31", ["W1"]),
    ('doi:"10.1234/a,b"', ["W1"]),
    ("doi:10.1234/shared", ["W2", "W3"]),
    ("ids.openalex:w1|https://openalex.org/W3", ["W1", "W3"]),
    ("authorships.author.id:A1,authorships.institutions.id:I2", ["W1"]),
    ("institutions.country_code:us+gb", ["W1"]),
    ("authorships.author.id:!A1", ["W2", "W3"]),
    ("open_access.is_oa:true", ["W1"]),
    ("open_access.is_oa:null", ["W2", "W3"]),
    ("has_abstract:false", ["W1", "W2", "W3"]),
    ("authors_count:>1", ["W1"]),
    ("cites:W2", ["W1"]),
    ("cited_by:W2", ["W3"]),
    ("related_to:W1", ["W3"]),
    ("cites:W2,publication_year:2020", ["W1"]),
    ("cited_by:!W2", ["W1", "W2"]),
])
def test_official_filters_count_before_paging(client, filters, ids):
    response = client.get("/works", params={"filter": filters, "sort": "id", "select": "id,title,doi"})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["meta"]["count"] == len(ids)
    assert [r["id"].split("/")[-1] for r in data["results"]] == ids
    assert all(set(r) == {"id", "title", "doi"} for r in data["results"])


def test_cursor_paging_is_bound_and_has_no_gaps(client):
    params = {"sort": "publication_year:desc", "per-page": 1, "cursor": "*"}
    seen = []
    while True:
        response = client.get("/works", params=params)
        assert response.status_code == 200, response.text
        data = response.json()
        seen.extend(r["id"].split("/")[-1] for r in data["results"])
        if data["meta"]["next_cursor"] is None:
            break
        params["cursor"] = data["meta"]["next_cursor"]
        assert client.get("/authors", params=params).status_code == 400
    assert seen == ["W2", "W1", "W3"]


def test_group_by_exact_counts_and_unknowns(client):
    params = {"group_by": "publication_year:include_unknown", "per_page": 200, "sort": "key:asc"}
    response = client.get("/works", params=params)
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["meta"]["count"] == 3
    assert {r["key"]: r["count"] for r in data["group_by"]} == {"2020": 1, "2021": 1, "-111": 1}
    response = client.get("/works", params={"group_by": "authorships.institutions.id"})
    assert response.status_code == 200, response.text
    assert {r["key"]: r["count"] for r in response.json()["group_by"]} == {"https://openalex.org/I1": 1, "https://openalex.org/I2": 1}
    response = client.get("/works", params={"group_by": "has_doi,publication_year:include_unknown"})
    assert response.status_code == 200, response.text
    buckets = response.json()["group_by"]
    assert buckets[0]["key"] == "true" and buckets[0]["count"] == 3
    assert {r["key"]: r["count"] for r in buckets[0]["groups"]} == {"2020": 1, "2021": 1, "-111": 1}


def test_quoted_filter_literals_and_null_or(client):
    for filters, expected in [('title:"Alpha, beta"', 1), ('title:"C++"', 0),
                              ('title:"Alpha|beta"', 0), ('publication_year:null|2020', 2)]:
        response = client.get("/works", params={"filter": filters})
        assert response.status_code == 200, response.text
        assert response.json()["meta"]["count"] == expected


def test_singletons_aliases_and_local_route_compatibility(client):
    for path in ("/works/w1", "/works/https://openalex.org/W1", "/works/doi:10.1234/a,b"):
        r = client.get(path)
        assert r.status_code == 200, r.text
        assert r.json()["future_field"] == {"preserved": True}
    assert client.get("/works/doi:10.1234/shared").status_code == 200
    assert client.get("/v1/works/10.1234/shared").status_code == 409
    assert client.get("/authors/orcid:0000-0001-2345-6789").json()["display_name"] == "Author A"
    assert client.get("/institutions", params={"filter": "country_code:us"}).json()["meta"]["count"] == 1


def test_official_auth_styles_and_validation(client):
    key = config()["api_keys"][0]
    assert client.get("/works/W1", headers={"X-API-Key": "", "Authorization": "Bearer " + key}).status_code == 200
    assert client.get("/works/W1", headers={"X-API-Key": ""}, params={"api_key": key}).status_code == 200
    assert client.get("/works/W1", headers={"X-API-Key": ""}).status_code == 401
    for params in ({"per_page": 101}, {"page": 0}, {"sort": "bad"}, {"select": "authorships.author.id"},
                   {"filter": "bad:1"}, {"per-page": 10, "per_page": 10}, {"seed": 1}, {"page": 2, "cursor": "*"}):
        response = client.get("/works", params=params)
        assert response.status_code == 400, (params, response.text)
    r = client.get("/works", params={"search": "climate"})
    assert r.status_code == 503 and r.json()["detail"]["code"] == "search_not_ready"


def test_sample_is_seeded_and_paged(client):
    params = dict(sample=3, seed=123, per_page=2, select="id")
    one = client.get("/works", params=params).json()
    again = client.get("/works", params=params).json()
    two = client.get("/works", params=params | {"page": 2}).json()
    assert one["results"] == again["results"]
    assert len(one["results"]) == 2 and len(two["results"]) == 1
    assert len({r["id"] for r in one["results"] + two["results"]}) == 3
    response = client.get("/works/random", params={"select": "id,title"})
    assert response.status_code == 200 and set(response.json()) == {"id", "title"}
