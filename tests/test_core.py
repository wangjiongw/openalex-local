import time, pytest
from openalex_api.model import doi, canonical_id, numeric_id, abstract, project
from openalex_api.query import query, encode_cursor, decode_cursor
from openalex_api.backend import sql_string


@pytest.mark.parametrize(
    "value,expected",
    [
        ("https://doi.org/10.1000/ABC.(1)", "10.1000/abc.(1)"),
        ("DOI: 10.1000/A/B", "10.1000/a/b"),
        ("10.1000/a%2fb", "10.1000/a%2fb"),
        ("garbage", None),
    ],
)
def test_doi_preserves_punctuation(value, expected):
    assert doi(value) == expected


def test_string_numeric_ids():
    assert canonical_id("https://openalex.org/keywords/中文") == "keywords/中文"
    assert numeric_id("https://openalex.org/W123") == 123
    assert numeric_id("countries/CN") is None
    assert canonical_id("w123") == "W123"
    with pytest.raises(ValueError):
        numeric_id("W18446744073709551616")


def test_abstract():
    assert abstract({"研究": [1], "气候": [0, 2]}) == "气候 研究 气候"
    with pytest.raises(ValueError):
        abstract({"word": [-1]})


def test_affiliations_never_cross_product():
    r = {
        "id": "W1",
        "title": "气候变化",
        "unknown_new_field": {"x": None},
        "authorships": [
            {
                "author": {"id": "A1", "display_name": "甲"},
                "institutions": [{"id": "I1", "display_name": "一大学"}],
            },
            {
                "author": {"id": "A2", "display_name": "乙"},
                "institutions": [{"id": "I2", "display_name": "二大学"}],
            },
        ],
    }
    p = project("works", r)
    assert p["authorships"][0]["institution_id"] == ["I1"]
    assert p["authorships"][1]["author_id"] == "A2"
    assert p["is_xpac"] is None
    q = query("works", filters="author:A1,institution:I2")
    nested = q["query"]["bool"]["filter"][0]["nested"]["query"]["bool"]["must"]
    assert len(nested) == 2
    assert r["unknown_new_field"] == {"x": None}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"filters": "unsupported:x"},
        {"filters": "is_oa:yes"},
        {"filters": "year:hello"},
        {"filters": "year:.."},
        {"filters": "date:2025-99-01"},
        {"sort": "arbitrary:desc"},
        {"sort": "id:sideways"},
        {"search_field": "semantic"},
        {"corpus": "maybe"},
        {"group_by": "madeup"},
    ],
)
def test_reject_unknown_combinations(kwargs):
    with pytest.raises(ValueError):
        query("works", **kwargs)


def test_filter_sent_with_text():
    q = query(
        "works",
        search="climate",
        filters="year:2020..2025,type:article|book,is_oa:!null",
    )
    assert q["query"]["bool"]["must"] and len(q["query"]["bool"]["filter"]) == 3
    assert q["track_total_hits"] is True


def test_negative_authorship():
    q = query("works", filters="author:!A1")
    assert q["query"]["bool"]["filter"][0]["bool"]["must_not"][0]["nested"][
        "query"
    ] == {"term": {"authorships.author_id": "A1"}}


def test_cursor_tamper_and_expiry():
    data = {
        "expires": time.time() + 60,
        "generation": "test",
        "query": "abc",
        "after": [1],
    }
    token = encode_cursor(data, "secret")
    assert decode_cursor(token, "secret") == data
    with pytest.raises(ValueError):
        decode_cursor(token, "other")
    with pytest.raises(ValueError):
        decode_cursor(encode_cursor(data | {"expires": 0}, "secret"), "secret")


def test_sql_escape():
    assert sql_string("a' OR 1=1 --\\") == "'a\\' OR 1=1 --\\\\'"


def test_missing_author_means_no_author_anywhere():
    q = query("works", filters="author:null")
    assert q["query"]["bool"]["filter"][0]["bool"]["must_not"][0]["nested"][
        "query"
    ] == {"exists": {"field": "authorships.author_id"}}


def test_generation_operations_never_use_prefix_wildcards():
    from openalex_api.backend import index_expression
    from openalex_api.config import ENTITIES

    names = index_expression("pilot").split(",")
    assert len(names) == len(ENTITIES)
    assert "*" not in index_expression("pilot")
    assert set(names).isdisjoint(index_expression("pilot_v2").split(","))
