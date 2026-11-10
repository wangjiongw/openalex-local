"""Official REST query vocabulary over the complete, immutable source metadata.

The identifier and citation predicates use the existing sorted tables. Other
predicates run inside ClickHouse, before counting, sorting, or pagination.
"""

import datetime
import json
import math
import re
import secrets
import time

from .backend import BackendError, database, sql_list, sql_string
from .config import ENTITIES, config, generation_path, read_generation
from .model import canonical_id, doi
from .query import decode_cursor, encode_cursor, fingerprint


COMMON = {
    "id": "id", "display_name": "str", "cited_by_count": "int",
    "works_count": "int", "created_date": "date", "updated_date": "date",
    "summary_stats.2yr_mean_citedness": "float", "summary_stats.h_index": "int",
    "summary_stats.i10_index": "int",
}
WORKS = {
    **dict.fromkeys(("title", "type", "type_crossref", "language", "indexed_in",
                     "open_access.oa_status", "biblio.volume", "biblio.issue",
                     "biblio.first_page", "biblio.last_page"), "str"),
    **dict.fromkeys(("publication_year", "referenced_works_count", "locations_count"), "int"),
    "publication_date": "date", "fwci": "float", "authors_count": "length",
    **dict.fromkeys(("is_retracted", "is_paratext", "is_xpac", "has_fulltext",
                     "open_access.is_oa", "open_access.any_repository_has_fulltext",
                     "authorships.is_corresponding"), "bool"),
    "authorships.author.id": "id", "authorships.author.orcid": "orcid",
    "authorships.institutions.id": "id", "authorships.institutions.lineage": "id",
    "authorships.institutions.ror": "ror", "authorships.institutions.country_code": "str",
    "authorships.institutions.type": "str", "authorships.countries": "str",
    **dict.fromkeys(("topics.id", "keywords.id", "concepts.id", "funders.id", "awards.id",
                     "referenced_works", "related_works", "sustainable_development_goals.id",
                     "grants.funder"), "id"),
    "grants.award_id": "str", "doi": "doi", "ids.doi": "doi", "ids.pmid": "pmid",
    "ids.pmcid": "pmcid", "citation_normalized_percentile.value": "float",
    "citation_normalized_percentile.is_in_top_1_percent": "bool",
    "citation_normalized_percentile.is_in_top_10_percent": "bool",
}
for prefix in ("primary_location", "best_oa_location", "locations"):
    WORKS.update({prefix + "." + k: v for k, v in {
        "source.id": "id", "source.issn": "str", "source.issn_l": "str",
        "source.type": "str", "source.host_organization": "id",
        "source.host_organization_lineage": "id", "source.is_oa": "bool",
        "source.is_in_doaj": "bool", "license": "str", "version": "str",
        "is_oa": "bool", "is_accepted": "bool", "is_published": "bool",
        "pdf_url": "str", "landing_page_url": "str",
    }.items()})
for prefix in ("topics", "primary_topic"):
    WORKS.update({prefix + "." + suffix: "id" for suffix in
                  ("id", "domain.id", "field.id", "subfield.id")})
for prefix in ("apc_list", "apc_paid"):
    WORKS.update({prefix + ".value": "float", prefix + ".value_usd": "float",
                  prefix + ".currency": "str", prefix + ".provenance": "str"})

OTHER = {
    "authors": {"orcid": "orcid", "last_known_institutions.id": "id",
                "last_known_institutions.country_code": "str",
                "last_known_institutions.type": "str", "last_known_institutions.lineage": "id"},
    "institutions": {"ror": "ror", "ids.ror": "ror", "country_code": "str",
                     "type": "str", "lineage": "id", "continent": "str",
                     "is_global_south": "bool", "geo.city": "str", "geo.region": "str",
                     "geo.country_code": "str", "repositories.id": "id"},
    "sources": {"issn": "issn", "issn_l": "issn", "ids.issn": "issn",
                "ids.issn_l": "issn", "host_organization": "id", "host_organization_lineage": "id",
                "type": "str", "country_code": "str", "is_oa": "bool", "is_in_doaj": "bool",
                "is_core": "bool", "is_global_south": "bool", "apc_usd": "float"},
    "publishers": {"country_codes": "str", "hierarchy_level": "int", "parent_publisher": "id",
                   "lineage": "id", "ror": "ror", "ids.ror": "ror"},
    "funders": {"country_code": "str", "is_global_south": "bool", "ror": "ror",
                "ids.ror": "ror", "ids.crossref": "str", "awards_count": "int"},
    "awards": {"funder.id": "id", "funder_award_id": "str", "amount": "float",
               "currency": "str", "start_year": "int", "end_year": "int"},
    "topics": {"domain.id": "id", "field.id": "id", "subfield.id": "id"},
    "subfields": {"domain.id": "id", "field.id": "id"},
    "fields": {"domain.id": "id"},
    "concepts": {"level": "int", "ancestors.id": "id"},
}
ALIASES = {
    "openalex": "id", "openalex_id": "id", "ids.openalex": "id",
    "is_oa": "open_access.is_oa", "oa_status": "open_access.oa_status",
    "author.id": "authorships.author.id", "author.orcid": "authorships.author.orcid",
    "institutions.id": "authorships.institutions.id",
    "institutions.country_code": "authorships.institutions.country_code",
    "institutions.lineage": "authorships.institutions.lineage",
    "institutions.ror": "authorships.institutions.ror",
    "institutions.type": "authorships.institutions.type",
    "last_known_institution.id": "last_known_institutions.id",
}
ARRAYS = {"authorships", "institutions", "countries", "locations", "topics", "keywords", "concepts",
          "funders", "awards", "grants", "referenced_works", "related_works", "issn", "country_codes",
          "lineage", "host_organization_lineage", "last_known_institutions", "ancestors",
          "repositories", "sustainable_development_goals", "indexed_in"}
HAS = {"has_doi": "doi", "has_orcid": "orcid", "has_ror": "ror",
       "has_abstract": "abstract_inverted_index", "has_references": "referenced_works",
       "has_pdf_url": "locations.pdf_url", "has_oa_accepted_version": "oa_accepted",
       "has_oa_published_version": "oa_published"}
EXTERNAL = {"works": {"doi", "pmid", "pmcid"}, "authors": {"orcid"},
            "institutions": {"ror", "wikidata"}, "sources": {"issn", "issn_l"},
            "publishers": {"ror", "wikidata"}, "funders": {"ror", "doi", "crossref", "wikidata"},
            "concepts": {"wikidata"}}
PARAMS = {"filter", "sort", "select", "page", "per_page", "per-page", "cursor", "group_by", "group-by",
          "sample", "seed", "api_key", "mailto"}


def fields(entity):
    result = COMMON | (WORKS if entity == "works" else OTHER.get(entity, {}))
    for scheme in EXTERNAL.get(entity, set()):
        result.setdefault(scheme, scheme if scheme in {"doi", "orcid", "ror", "issn", "pmid", "pmcid"} else "str")
        result.setdefault("ids." + scheme, result[scheme])
    return result


def has_fields(entity):
    return {key: path for key, path in HAS.items() if path in fields(entity)
            or entity == "works" and key not in ("has_orcid", "has_ror")}


def split_filters(text):
    """A quoted identifier may contain commas; pasted search titles may too."""
    parts, start, quoted, escaped = [], 0, False, False
    for i, char in enumerate(text):
        if escaped:
            escaped = False
        elif char == "\\" and quoted:
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif char == "," and not quoted:
            current = text[start:i]
            key = current.partition(":")[0]
            if ".search" in key and not re.match(r"[a-zA-Z_][\w.]*:", text[i + 1:]):
                continue
            parts.append(current)
            start = i + 1
    if quoted:
        raise ValueError("unclosed quote in filter")
    if text:
        parts.append(text[start:])
    result = []
    for part in parts:
        key, sep, value = part.partition(":")
        if not sep or not key or not value:
            raise ValueError("filter syntax is field:value,field:value")
        result.append((key.lower(), value))
    return result


def unquote(value):
    if value.startswith('"') and value.endswith('"'):
        return json.loads(value)
    return value


def split_values(value, separator):
    parts, start, quoted, escaped = [], 0, False, False
    for i, char in enumerate(value):
        if escaped:
            escaped = False
        elif char == "\\" and quoted:
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif char == separator and not quoted:
            parts.append(value[start:i])
            start = i + 1
    return parts + [value[start:]]


def entity_id(entity, value):
    key = canonical_id(value)
    if entity not in ("works", "authors", "sources", "institutions", "topics", "publishers", "funders", "awards", "concepts") and "/" not in key:
        key = entity + "/" + key
    return key


PARAMS.add("search_scope")

def field_name(entity, key):
    if key in ("is_oa", "oa_status") and entity != "works":
        return key
    if entity != "works" and key.startswith("institutions."):
        return key
    return ALIASES.get(key, key)


def values_sql(path):
    parts = [p + ("[*]" if p in ARRAYS else "") for p in path.split(".")]
    return "JSONExtractArrayRaw(JSON_QUERY(raw," + sql_string("$." + ".".join(parts)) + "))"


def converted(value, kind):
    value = unquote(value)
    if kind in ("int", "length"):
        number = int(value)
        if not -(2**63) <= number < 2**64:
            raise ValueError("integer outside supported 64-bit range")
        return str(number)
    if kind == "float":
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("numeric value must be finite")
        return str(number)
    if kind == "bool":
        if value.lower() not in ("true", "false"):
            raise ValueError("boolean value must be true or false")
        return sql_string(value.lower())
    if kind == "date":
        datetime.date.fromisoformat(value[:10])
    if kind == "doi":
        value = doi(value)
        if not value:
            raise ValueError("invalid DOI")
        value = "https://doi.org/" + value
    elif kind == "id":
        if not re.match(r"https?://", value) or "openalex.org/" in value.lower():
            value = "https://openalex.org/" + canonical_id(value)
    elif kind in ("orcid", "ror", "pmid", "pmcid"):
        value = value.rstrip("/").split("/")[-1]
    return sql_string(value.lower())


def scalar_sql(path, kind, variable="v"):
    if path in ("id", "publication_year", "cited_by_count"):
        return path
    if path == "authors_count":
        return "length(JSONExtractArrayRaw(raw,'authorships'))"
    if kind in ("int", "float", "length"):
        return f"toFloat64OrNull({variable})"
    if kind == "bool":
        return variable
    value = f"lowerUTF8(JSONExtractString({variable}))"
    if kind == "date":
        value = f"substring({value},1,10)"
    if kind in ("orcid", "ror", "pmid", "pmcid"):
        value = f"arrayElement(splitByChar('/',{value}),-1)"
    return value


def comparison(expr, value, kind):
    if value in ("null", "!null"):
        return f"{expr} IS {'NOT ' if value.startswith('!') else ''}NULL"
    match = re.fullmatch(r"(>=|<=|>|<)(.+)", value)
    if match:
        if kind not in ("int", "float", "date", "length"):
            raise ValueError("inequality requires a number or date")
        return f"{expr}{match[1]}{converted(match[2], kind)}"
    if kind in ("int", "float", "length", "date"):
        pattern = r"(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})" if kind == "date" else r"(-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)"
        bounds = re.fullmatch(pattern, value)
        if bounds:
            return f"({expr}>={converted(bounds[1], kind)} AND {expr}<={converted(bounds[2], kind)})"
    return f"{expr}={converted(value, kind)}"


def predicate(entity, key, value):
    key = field_name(entity, key)
    if key in has_fields(entity):
        if HAS[key] in ("oa_accepted", "oa_published"):
            version = "acceptedVersion" if HAS[key] == "oa_accepted" else "publishedVersion"
            exists = "arrayExists(l -> JSONExtractBool(l,'is_oa') AND JSONExtractString(l,'version')=" + sql_string(version) + ",JSONExtractArrayRaw(raw,'locations'))"
        else:
            exists = "arrayExists(v -> v NOT IN ('null','\"\"','[]','{}')," + values_sql(HAS[key]) + ")"
        negate = value.startswith("!")
        boolean = unquote(value[1:] if negate else value)
        converted(boolean, "bool")
        return ("NOT " if (boolean.lower() == "false") != negate else "") + exists
    registry = fields(entity)
    if key not in registry:
        raise ValueError("unsupported filter for " + entity + ": " + key)
    kind = registry[key]
    negate = value.startswith("!")
    value = value[1:] if negate else value
    options = split_values(value, "|")
    if len(options) > 100:
        raise ValueError("maximum 100 OR values")
    if not value:
        raise ValueError("empty filter value")
    intersections = split_values(value, "+")
    if len(intersections) > 1 and kind not in ("doi", "int", "float", "length", "date", "bool"):
        result = "(" + " AND ".join(predicate(entity, key, v) for v in intersections) + ")"
        return "NOT " + result if negate else result
    if len(options) > 1 and "null" in options:
        clause = "(" + " OR ".join(predicate(entity, key, v) for v in options) + ")"
        return "NOT ifNull((" + clause + "),0)" if negate else clause
    if kind == "id":
        family = next((plural for singular, plural in (("domain", "domains"), ("field", "fields"), ("subfield", "subfields"))
                       if key.endswith(singular + ".id")), None)
        if family:
            options = [family + "/" + v if v.isdigit() else v for v in options]
        elif key == "sustainable_development_goals.id":
            options = ["https://metadata.un.org/sdg/" + canonical_id(unquote(v)).split("/")[-1] for v in options]
    if key == "id":
        clause = "id IS NULL" if value == "null" else "id IN " + sql_list([entity_id(entity, unquote(v)) for v in options])
    elif key in ("publication_year", "cited_by_count", "authors_count"):
        expr = scalar_sql(key, kind)
        clause = "(" + " OR ".join(comparison(expr, v, kind) for v in options) + ")"
    elif value == "null":
        clause = "NOT arrayExists(v -> v NOT IN ('null','\"\"','[]','{}')," + values_sql(key) + ")"
    else:
        expr = scalar_sql(key, kind)
        clause = "arrayExists(v -> " + " OR ".join(comparison(expr, v, kind) for v in options) + "," + values_sql(key) + ")"
    return "NOT ifNull((" + clause + "),0)" if negate else clause


def identifier(entity, value):
    allowed = EXTERNAL.get(entity, set())
    scheme, sep, token = value.partition(":")
    if sep and scheme.lower() in allowed:
        scheme = scheme.lower()
    elif doi(value) and "doi" in allowed:
        scheme, token = "doi", value
    else:
        urls = {"orcid": r"https?://orcid\.org/", "ror": r"https?://ror\.org/",
                "wikidata": r"https?://(?:www\.)?wikidata\.org/(?:wiki|entity)/",
                "pmid": r"https?://(?:pubmed\.ncbi\.nlm\.nih\.gov/|www\.ncbi\.nlm\.nih\.gov/pubmed/)"}
        found = next((s for s, pattern in urls.items() if s in allowed and re.match(pattern, value, re.I)), None)
        if not found:
            return "openalex", entity_id(entity, value)
        scheme, token = found, value
    if scheme == "doi":
        token = doi(token)
        if not token:
            raise ValueError("invalid DOI")
    elif scheme == "issn_l":
        scheme = "issn"
    if scheme in ("orcid", "ror", "pmid", "pmcid"):
        token = token.rstrip("/").split("/")[-1]
    if scheme == "wikidata":
        token = "https://www.wikidata.org/wiki/" + token.rstrip("/").split("/")[-1]
    return scheme, token.strip().lower()


def external_values(entity, scheme, values):
    tokens = [identifier(entity, scheme + ":" + unquote(value))[1] for value in values]
    if scheme == "wikidata":
        tokens = [prefix + token.rsplit("/", 1)[-1] for token in tokens
                  for prefix in ("https://www.wikidata.org/wiki/", "http://www.wikidata.org/wiki/",
                                 "https://www.wikidata.org/entity/", "http://www.wikidata.org/entity/")]
    return tokens


def where_sql(g, entity, filters):
    clauses = ["entity=" + sql_string(entity)]
    for key, value in filters:
        canonical = field_name(entity, key)
        if key.startswith(("from_", "to_")):
            prefix, path = key.split("_", 1)
            if path not in ("publication_date", "created_date", "updated_date"):
                raise ValueError("unsupported date filter: " + key)
            clauses.append(predicate(entity, path, (">=" if prefix == "from" else "<=") + value))
        elif entity == "works" and key in ("cites", "cited_by", "related_to"):
            negate = value.startswith("!")
            values = value.lstrip("!").split("|")
            if len(values) > 100 or not all(re.fullmatch(r"W\d+", canonical_id(v), re.I) for v in values):
                raise ValueError(key + " requires up to 100 work IDs")
            keys = [canonical_id(v) for v in values]
            db = database(g)
            if key == "related_to":
                sub = f"SELECT arrayJoin(arrayMap(x -> replaceRegexpOne(x,'^https?://openalex.org/',''),JSONExtract(raw,'related_works','Array(String)'))) FROM {db}.entities FINAL WHERE entity='works' AND id IN {sql_list(keys)}"
            else:
                table, fixed, target = ("references_in", "cited", "citing") if key == "cites" else ("references_out", "citing", "cited")
                nums = "(" + ",".join(str(int(k[1:])) for k in keys) + ")"
                sub = f"SELECT concat('W',toString({target})) FROM {db}.{table} FINAL WHERE {fixed} IN {nums}"
            clauses.append("id " + ("NOT " if negate else "") + "IN (" + sub + ")")
        elif canonical.removeprefix("ids.") in EXTERNAL.get(entity, set()) and value not in ("null", "!null"):
            if canonical not in fields(entity):
                raise ValueError("unsupported filter for " + entity + ": " + key)
            negate = value.startswith("!")
            scheme = canonical.removeprefix("ids.").replace("issn_l", "issn")
            options = split_values(value.lstrip("!"), "|")
            if len(options) > 100:
                raise ValueError("maximum 100 OR values")
            tokens = external_values(entity, scheme, options)
            clauses.append(f"id {'NOT ' if negate else ''}IN (SELECT id FROM {database(g)}.identifiers FINAL WHERE entity={sql_string(entity)} AND scheme={sql_string(scheme)} AND value IN {sql_list(tokens)})")
        else:
            clauses.append(predicate(entity, key, value))
    return " AND ".join("(" + c + ")" for c in clauses)


def sort_fields(entity, text):
    result = []
    for part in (text or "id:asc").split(","):
        name, sep, direction = part.partition(":")
        name = field_name(entity, name)
        direction = direction.lower() if sep else "asc"
        registry = fields(entity)
        if name not in registry or direction not in ("asc", "desc") or any(p in ARRAYS for p in name.split(".")):
            raise ValueError("unsupported sort: " + part)
        kind = registry[name]
        expr = name if name in ("id", "publication_year", "cited_by_count") else "arrayElementOrNull(arrayMap(v -> " + scalar_sql(name, kind) + "," + values_sql(name) + "),1)"
        result.append((name, expr, direction))
    if not any(name == "id" for name, _, _ in result):
        result.append(("id", "id", "asc"))
    return result


def seek_sql(sorts, after):
    if len(sorts) != len(after):
        raise ValueError("cursor sort mismatch")
    options, equal = [], []
    for (_, expr, direction), value in zip(sorts, after):
        if value is None:
            equal.append(f"isNull({expr})")
            continue
        token = str(value) if isinstance(value, (int, float)) else sql_string(value)
        later = f"({expr}{'>' if direction == 'asc' else '<'}{token} OR isNull({expr}))"
        options.append("(" + " AND ".join(equal + [later]) + ")")
        equal.append(f"{expr}={token}")
    return "(" + " OR ".join(options) + ")" if options else "0"


def parameters(request):
    params = dict(request.query_params)
    if len(params) != len(request.query_params.multi_items()):
        raise ValueError("duplicate query parameters are not supported")
    for hyphen, underscore in (("per-page", "per_page"), ("group-by", "group_by")):
        if hyphen in params:
            if underscore in params:
                raise ValueError("use only one spelling of " + underscore)
            params[underscore] = params.pop(hyphen)
    return params


def source_table(b, meta, entity):
    table = database(meta["generation"]) + ".entities"
    receipt = generation_path(meta["generation"]) / "metadata-release.json"
    if meta.get("metadata_frozen") and receipt.exists():
        from .recovery import require_evidence

        release = json.loads(receipt.read_text())
        try:
            proof = require_evidence(meta["generation"], "metadata-complete")
        except RuntimeError:
            return table + " FINAL"
        if release.get("passed") and release.get("metadata_signature") == proof["signature"]:
            expected = int(release["entities"][entity]["unique_entities"])
            rows = b.ch("SELECT sum(rows) n FROM system.parts WHERE active AND database="
                + sql_string(database(meta["generation"])) + " AND table='entities' AND partition="
                + sql_string(entity) + " FORMAT JSONEachRow")
            # FINAL is redundant only when frozen, audited unique and physical counts agree.
            if int(rows[0]["n"]) == expected:
                return table
    return table + " FINAL"


def execute(b, meta, entity, params):
    started = time.monotonic()
    from .search_scopes import validate_scope
    validate_scope(params.get("search_scope", "available"))
    from .official_text import SCOPES, FILTER_SEARCH
    unknown = set(params) - PARAMS - set(SCOPES)
    if unknown:
        raise ValueError("unsupported parameters: " + ", ".join(sorted(unknown)))
    page = int(params.get("page", 1))
    per_page = int(params.get("per_page", 25))
    group = params.get("group_by")
    if page < 1 or not 1 <= per_page <= (200 if group else 100):
        raise ValueError("invalid page or per_page")
    cursor = params.get("cursor")
    if cursor and "page" in params:
        raise ValueError("cursor and page cannot be combined")
    if not cursor and page * per_page > 10000:
        raise ValueError("use cursor beyond the first 10,000 results")
    sample = int(params["sample"]) if "sample" in params else None
    if sample is not None and (not 1 <= sample <= 10000 or cursor or group):
        raise ValueError("sample must be 1-10000 and cannot combine with cursor or group_by")
    if "seed" in params and sample is None:
        raise ValueError("seed requires sample")
    if group and params.get("select"):
        raise ValueError("select cannot be combined with group_by")
    from .api import selected
    selected({}, params.get("select"))
    if len(params.get("filter", "")) > 64000:
        raise ValueError("filter exceeds 64000 characters")
    filters = split_filters(params.get("filter", ""))
    from .official_text import can_route
    cursor_is_sql = bool(cursor and cursor != "*" and decode_cursor(cursor, config()["cursor_secret"]).get("kind") == "official")
    manifest = meta.get("_search_build") or {}
    full_entity = not manifest.get("incremental") or manifest.get("entities", {}).get(entity, {}).get("count") == read_generation(meta["generation"])["expected"][entity]["records"]
    indexed = bool(manifest) and full_entity and not cursor_is_sql and can_route(entity, params, filters)
    if manifest.get("engine") == "manticore" and entity != "works":
        indexed = False
    if indexed or "search_scope" in params or any(k in SCOPES for k in params) or any(k in FILTER_SEARCH for k, _ in filters):
        from .api import require_search, search_listing
        from .official_text import build

        require_search(meta, entity)
        if group or sample:
            raise ValueError("text search with group_by or sample is not implemented yet")
        body = build(entity, params, filters)
        if entity in ("authors", "institutions"):
            from .catalogs import include_aliases
            include_aliases(body["query"])
        result = search_listing(entity, body, params.get("select"), page, per_page, cursor, scores=True, search_scope=params.get("search_scope", "available"))
        result["group_by"] = []
        result["meta"].update(page=None if cursor else page, groups_count=None,
            db_response_time_ms=round((time.monotonic() - started) * 1000),
            text_content_scope=("local snapshot title, abstract and explicit keywords; no upstream fulltext" if manifest.get("engine") == "manticore" else "local snapshot title and abstract; no upstream fulltext or keyword expansion"))
        return result
    fp = fingerprint(dict(entity=entity, params={k: v for k, v in params.items() if k not in ("cursor", "page", "api_key", "mailto")}))
    payload = None
    if cursor and cursor != "*":
        payload = decode_cursor(cursor, config()["cursor_secret"])
        if payload.get("kind") != "official" or payload["query"] != fp:
            raise ValueError("cursor belongs to a different query")
        meta = read_generation(payload["generation"])
    g = meta["generation"]
    if group and "," in group:
        parts = group.split(",")
        if not 2 <= len(parts) <= 3 or cursor or page != 1:
            raise ValueError("multi-field grouping supports 2-3 fields without cursor/page > 1")
        for part in parts:
            key = field_name(entity, part.split(":")[0])
            if key not in fields(entity) and key not in has_fields(entity):
                raise ValueError("unsupported group_by: " + part)
        result = execute(b, meta, entity, params | {"group_by": parts[0]})
        key = field_name(entity, parts[0].split(":")[0])
        for bucket in result["group_by"]:
            value = str(bucket["key"])
            kind = fields(entity).get(key, "bool")
            if value == "unknown" or value == "-111" and kind in ("int", "float", "length"):
                value = "null"
            else:
                value = json.dumps(value)
            extra = key + ":" + value
            child = execute(b, meta, entity, params | {"group_by": ",".join(parts[1:]),
                "filter": params.get("filter", "") + ("," if params.get("filter") else "") + extra})
            bucket["groups"] = child["group_by"]
        result["meta"]["db_response_time_ms"] = round((time.monotonic() - started) * 1000)
        return result
    where = where_sql(g, entity, filters)
    table = source_table(b, meta, entity)
    # Frozen release counts avoid a full-table FINAL scan for an unfiltered list.
    receipt = generation_path(g) / "metadata-release.json"
    if not filters and meta.get("metadata_frozen") and receipt.exists():
        total = int(json.loads(receipt.read_text())["entities"][entity]["unique_entities"])
    else:
        total = int(b.ch(f"SELECT count() n FROM {table} WHERE {where} FORMAT JSONEachRow")[0]["n"])
    response_meta = dict(count=min(total, sample) if sample else total, db_response_time_ms=0,
                         page=None if cursor else page, per_page=per_page, groups_count=None,
                         generation=g)
    expires = payload["expires"] if payload else time.time() + config()["cursor_ttl"]
    next_cursor = None
    if group:
        if page != 1:
            raise ValueError("group_by supports cursor paging, not page > 1")
        key, _, modifier = group.partition(":")
        key = field_name(entity, key)
        if key not in fields(entity) and key not in has_fields(entity) or modifier not in ("", "include_unknown"):
            raise ValueError("unsupported group_by: " + group)
        kind = fields(entity).get(key, "bool")
        if key in has_fields(entity):
            expr = "[if(" + predicate(entity, key, "true") + ",'true','false')]"
        elif key == "id":
            expr = "[concat('https://openalex.org/',id)]"
        else:
            expr = "[toString(" + scalar_sql(key, kind) + ")]" if key in ("publication_year", "cited_by_count", "authors_count") else "arrayMap(v -> if(v='null','',if(startsWith(v,'\"'),JSONExtractString(v),v))," + values_sql(key) + ")"
        unknown = "-111" if kind in ("int", "float", "length") else "unknown"
        expr = f"arrayDistinct(arrayFilter(v -> v!='', {expr}))"
        if modifier:
            expr = f"if(empty({expr}),[{sql_string(unknown)}],{expr})"
        having = ""
        if payload:
            having = " HAVING key>" + sql_string(payload["after"])
        order = params.get("sort", "count:desc")
        if order not in ("count:desc", "count:asc", "key:asc", "key:desc"):
            raise ValueError("group sort must be count:asc|desc or key:asc|desc")
        col, direction = order.split(":")
        order_sql = "key ASC" if cursor else ("n " + direction + ",key ASC" if col == "count" else "key " + direction)
        rows = b.ch(f"SELECT arrayJoin({expr}) key,uniqExact(id) n FROM {table} WHERE {where} GROUP BY key{having} ORDER BY {order_sql} LIMIT {per_page + 1} FORMAT JSONEachRow")
        more = len(rows) > per_page
        rows = rows[:per_page]
        groups = [dict(key=r["key"], key_display_name=r["key"], count=int(r["n"])) for r in rows]
        prefixes = {"W": "works", "A": "authors", "I": "institutions", "S": "sources", "T": "topics", "C": "concepts", "P": "publishers", "F": "funders", "G": "awards"}
        by_entity = {}
        for item in groups:
            k = canonical_id(item["key"])
            if re.fullmatch(r"[WAISTCPFG]\d+", k):
                by_entity.setdefault(prefixes[k[0]], []).append(k)
        for e, keys in by_entity.items():
            details = b.details(g, e, keys)
            for item in groups:
                doc = details.get(canonical_id(item["key"]))
                if doc:
                    item["key_display_name"] = doc.get("display_name") or doc.get("title") or item["key"]
        if cursor and more:
            next_cursor = rows[-1]["key"]
        response_meta["groups_count"] = len(groups) or None
        result = {"meta": response_meta, "results": [], "group_by": groups}
    else:
        sorts = sort_fields(entity, params.get("sort"))
        if sample:
            if params.get("sort"):
                raise ValueError("sample cannot combine with sort")
            seed = int(params.get("seed", secrets.randbits(32)))
            sorts = [("sample", "cityHash64(concat(id," + sql_string(seed) + "))", "asc"), ("id", "id", "asc")]
        if payload:
            where += " AND " + seek_sql(sorts, payload["after"])
        offset = (page - 1) * per_page
        size = min(per_page + 1, max(0, sample - offset)) if sample else per_page + 1
        columns = ",".join(expr + " AS s" + str(i) for i, (_, expr, _) in enumerate(sorts))
        order = ",".join("s" + str(i) + " " + direction + " NULLS LAST" for i, (_, _, direction) in enumerate(sorts))
        rows = b.ch(f"SELECT id,{columns} FROM {table} WHERE {where} ORDER BY {order} LIMIT {size} OFFSET {offset} FORMAT JSONEachRow") if size else []
        more = len(rows) > per_page
        rows = rows[:per_page]
        keys = [r["id"] for r in rows]
        details = b.details(g, entity, keys)
        if set(details) != set(keys):
            raise BackendError("query/details generation inconsistency")
        result = {"meta": response_meta, "results": [selected(details[k], params.get("select")) for k in keys], "group_by": []}
        if cursor and more:
            next_cursor = [rows[-1]["s" + str(i)] for i in range(len(sorts))]
    if cursor:
        response_meta["next_cursor"] = encode_cursor(dict(kind="official", generation=g, query=fp,
            after=next_cursor, expires=expires), config()["cursor_secret"]) if next_cursor is not None else None
    response_meta["db_response_time_ms"] = round((time.monotonic() - started) * 1000)
    return result
