"""Explicit, testable local query grammar; never post-filter a hit window."""

import base64, hashlib, hmac, json, math, time
from .model import canonical_id, normalize_title

FIELDS = {
    "year": ("publication_year", "int"),
    "date": ("publication_date", "date"),
    "type": ("type", "str"),
    "language": ("language", "str"),
    "cited_by_count": ("cited_by_count", "int"),
    "works_count": ("works_count", "int"),
    "fwci": ("fwci", "float"),
    "author": ("authorships.author_id", "id"),
    "orcid": ("authorships.orcid", "external"),
    "institution": ("authorships.institution_id", "id"),
    "ror": ("authorships.ror", "external"),
    "country": ("country", "country"),
    "source": ("source_id", "id"),
    "issn": ("issn", "lower"),
    "publisher": ("publisher_id", "id"),
    "topic": ("topic_id", "id"),
    "primary_topic": ("primary_topic_id", "id"),
    "primary_source": ("primary_source_id", "id"),
    "earth_group": ("earth_group", "str"),
    "keyword": ("keyword_id", "id"),
    "concept": ("concept_id", "id"),
    "funder": ("funder_id", "id"),
    "award": ("award_id", "id"),
    "domain": ("domain_id", "id"),
    "field": ("field_id", "id"),
    "subfield": ("subfield_id", "id"),
    "license": ("license", "str"),
    "oa_status": ("oa_status", "str"),
    "is_oa": ("is_oa", "bool"),
    "is_xpac": ("is_xpac", "bool"),
    "is_retracted": ("is_retracted", "bool"),
    "is_paratext": ("is_paratext", "bool"),
    "has_fulltext": ("has_fulltext", "bool"),
    "id": ("id", "id"),
    "name_exact": ("name_exact", "title"),
    "current_institution": ("institution_id", "id"),
}
WORK_ONLY = set(FIELDS) - {
    "type",
    "country",
    "cited_by_count",
    "works_count",
    "id",
    "name_exact",
    "current_institution",
}
SEARCH_FIELDS = {
    "all": [
        "name^5",
        "name.en^2",
        "name.zh^3",
        "abstract",
        "abstract.en",
        "abstract.zh",
    ],
    "title": ["name^5", "name.en^2", "name.zh^3"],
    "title.exact": ["name"],
    "abstract": ["abstract", "abstract.en", "abstract.zh"],
    "title_abstract_keywords": ["name", "name.en", "abstract", "abstract.en", "keywords", "keywords.en"],
    "author": ["authorships.author_name", "authorships.author_name.zh"],
    "institution": ["authorships.institution_name", "authorships.institution_name.zh"],
    "source": ["source_name", "source_name.zh"],
}
SORTS = {
    "relevance": "_score",
    "id": "id",
    "year": "publication_year",
    "cited_by_count": "cited_by_count",
    "works_count": "works_count",
    "date": "publication_date",
}
GROUPS = {
    "year": "publication_year",
    "type": "type",
    "language": "language",
    "country": "country",
    "is_oa": "is_oa",
    "domain": "domain_id",
    "field": "field_id",
    "subfield": "subfield_id",
    "topic": "topic_id",
}


def convert(v, kind):
    if kind == "int":
        return int(v)
    if kind == "float":
        value = float(v)
        if not math.isfinite(value):
            raise ValueError("numeric filter must be finite")
        return value
    if kind == "bool":
        if v not in ["true", "false"]:
            raise ValueError("boolean value must be true or false")
        return v == "true"
    if kind == "id":
        return canonical_id(v)
    if kind == "external":
        return v.rstrip("/").split("/")[-1].lower()
    if kind == "lower":
        return v.lower()
    if kind == "country":
        return v.upper()
    if kind == "title":
        return normalize_title(v)
    if kind == "date":
        import datetime

        datetime.date.fromisoformat(v)
    return v


def clause(field, kind, value):
    if value == "null":
        return {"bool": {"must_not": [{"exists": {"field": field}}]}}
    if value == "!null":
        return {"exists": {"field": field}}
    negate = value.startswith("!")
    value = value[1:] if negate else value
    vals = value.split("|")
    if len(vals) > 100:
        raise ValueError("maximum 100 OR values")
    cs = []
    for v in vals:
        if kind in ["int", "float", "date"] and (".." in v or v.startswith((">", "<"))):
            if ".." in v:
                lo, hi = v.split("..", 1)
                bounds = {}
                if lo:
                    bounds["gte"] = convert(lo, kind)
                if hi:
                    bounds["lte"] = convert(hi, kind)
                if not bounds:
                    raise ValueError("empty range")
            else:
                op = (
                    "gte"
                    if v.startswith(">=")
                    else "lte"
                    if v.startswith("<=")
                    else "gt"
                    if v.startswith(">")
                    else "lt"
                )
                bounds = {op: convert(v[2:] if "=" in v[:2] else v[1:], kind)}
            cs.append({"range": {field: bounds}})
        else:
            cs.append({"term": {field: convert(v, kind)}})
    c = cs[0] if len(cs) == 1 else {"bool": {"should": cs, "minimum_should_match": 1}}
    return {"bool": {"must_not": [c]}} if negate else c


def query(
    entity,
    search="",
    search_field="all",
    filters="",
    corpus="all",
    sort=None,
    group_by=None,
    author_search="",
):
    if author_search and (entity != "works" or search_field == "author"):
        raise ValueError("author_search requires works and a separate title/abstract search field")
    outer = []
    nested = []
    if corpus not in ["all", "core", "xpac"]:
        raise ValueError("corpus must be all, core or xpac")
    if corpus != "all":
        if entity != "works":
            raise ValueError("corpus applies only to works")
        outer.append({"term": {"is_xpac": corpus == "xpac"}})
    if filters:
        for item in filters.split(","):
            if ":" not in item:
                raise ValueError("filter syntax is field:value,field:value")
            key, value = item.split(":", 1)
            if key not in FIELDS:
                raise ValueError("unsupported filter: " + key)
            if entity != "works" and key in WORK_ONLY:
                raise ValueError("filter applies only to works: " + key)
            if entity == "works" and key == "current_institution":
                raise ValueError("use institution for publication-time affiliation")
            field, kind = FIELDS[key]
            c = clause(field, kind, value)
            # Negative nested filters exclude works having any matching authorship.
            if field.startswith("authorships.") and value == "null":
                outer.append(
                    {
                        "bool": {
                            "must_not": [
                                {
                                    "nested": {
                                        "path": "authorships",
                                        "query": {"exists": {"field": field}},
                                    }
                                }
                            ]
                        }
                    }
                )
            elif (
                field.startswith("authorships.")
                and value.startswith("!")
                and value != "!null"
            ):
                positive = clause(field, kind, value[1:])
                outer.append(
                    {
                        "bool": {
                            "must_not": [
                                {"nested": {"path": "authorships", "query": positive}}
                            ]
                        }
                    }
                )
            elif field.startswith("authorships."):
                nested.append(c)
            else:
                outer.append(c)
    must = []
    if search_field not in SEARCH_FIELDS:
        raise ValueError("unsupported search_field")
    if search:
        if entity != "works" and search_field not in ["all", "title", "title.exact"]:
            raise ValueError("search_field applies only to works")
        fs = (
            SEARCH_FIELDS[search_field]
            if entity == "works"
            else ["name^4", "name.zh^3", "aliases^2", "description"]
        )
        if entity != "works" and search_field == "title.exact":
            fs = ["name"]
        text = {
            "multi_match": {
                "query": search,
                "fields": fs,
                "type": "best_fields",
                "operator": "and",
            }
        }
        if fs[0].startswith("authorships."):
            nested.append(text)
        else:
            must.append(
                {
                    "bool": {
                        "should": [
                            text,
                            {
                                "multi_match": {
                                    "query": search,
                                    "fields": fs,
                                    "type": "phrase",
                                    "boost": 2,
                                }
                            },
                        ],
                        "minimum_should_match": 1,
                    }
                }
            )
    if author_search:
        from .official_text import text_query
        nested.append(text_query(author_search, ("authorships.author_name",), exact=True))
    if nested:
        outer.append(
            {"nested": {"path": "authorships", "query": {"bool": {"must": nested}}}}
        )
    q = (
        {"bool": {"must": must, "filter": outer}}
        if must or outer
        else {"match_all": {}}
    )
    sort = sort or ("relevance:desc" if search or author_search else "id:asc")
    ss = []
    for part in sort.split(","):
        k, _, direction = part.partition(":")
        direction = direction or "desc"
        if k not in SORTS or direction not in ["asc", "desc"]:
            raise ValueError("unsupported sort")
        if k == "relevance" and not (search or author_search):
            raise ValueError("relevance sort needs search")
        ss.append({SORTS[k]: {"order": direction}})
    if not any("id" in s for s in ss):
        ss.append({"id": {"order": "asc"}})
    result = {"query": q, "sort": ss, "track_total_hits": True}
    if group_by:
        if group_by not in GROUPS:
            raise ValueError("unsupported group_by")
        if entity != "works" and group_by not in ["type", "country"]:
            raise ValueError("group_by applies only to works")
        result["aggs"] = {
            "groups": {
                "terms": {
                    "field": GROUPS[group_by],
                    "size": 1000,
                    "show_term_doc_count_error": True,
                }
            }
        }
    return result


def fingerprint(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def encode_cursor(payload, secret):
    b = json.dumps(payload, separators=(",", ":")).encode()
    sig = hmac.new(secret.encode(), b, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig + b).decode().rstrip("=")


def decode_cursor(token, secret):
    try:
        if len(token) > 20000:
            raise ValueError()
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        sig, b = raw[:32], raw[32:]
        if not hmac.compare_digest(
            sig, hmac.new(secret.encode(), b, hashlib.sha256).digest()
        ):
            raise ValueError()
        data = json.loads(b)
        if data["expires"] < time.time():
            raise ValueError("cursor expired")
        return data
    except Exception as e:
        raise ValueError("invalid or expired cursor") from e
