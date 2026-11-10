"""Boolean text syntax and official aliases for the existing search projection."""
import json
import re

from . import query as local
from .model import doi, normalize_title
from .official import field_name, unquote, split_values, fields

SCOPES = {
    "search": ("name", "abstract"), "search.exact": ("name", "abstract"),
    "search.title": ("name",), "search.title.exact": ("name",),
    "search.title_and_abstract": ("name", "abstract"),
    "search.title_and_abstract.exact": ("name", "abstract"),
    "search.title_abstract_keywords": ("name", "abstract", "keywords"),
}
FILTER_SEARCH = {"title.search": "search.title", "title.search.exact": "search.title.exact",
                 "title_and_abstract.search": "search.title_and_abstract",
                 "title_and_abstract.search.exact": "search.title_and_abstract.exact",
                 "default.search": "search", "display_name.search": "search.title"}
PROJECTED = {
    "title": ("name_literal", "str"), "display_name": ("name_literal", "str"),
    "id": ("id", "id"), "publication_year": ("publication_year", "int"),
    "publication_date": ("publication_date", "date"), "type": ("type", "str"),
    "language": ("language", "str"), "fwci": ("fwci", "float"),
    "cited_by_count": ("cited_by_count", "int"), "works_count": ("works_count", "int"),
    "open_access.is_oa": ("is_oa", "bool"), "open_access.oa_status": ("oa_status", "str"),
    "is_retracted": ("is_retracted", "bool"), "is_paratext": ("is_paratext", "bool"),
    "is_xpac": ("is_xpac", "bool"), "has_fulltext": ("has_fulltext", "bool"),
    "authorships.author.id": ("authorships.author_id", "id"),
    "authorships.author.orcid": ("authorships.orcid", "external"),
    "authorships.institutions.id": ("authorships.institution_id", "id"),
    "authorships.institutions.ror": ("authorships.ror", "external"),
    "authorships.countries": ("authorships.country", "country"),
    "country_code": ("country", "country"), "locations.source.id": ("source_id", "id"),
    "topics.id": ("topic_id", "id"), "concepts.id": ("concept_id", "id"),
    "primary_topic.id": ("primary_topic_id", "id"),
    "primary_location.source.id": ("primary_source_id", "id"),
    "keywords.id": ("keyword_id", "id"), "funders.id": ("funder_id", "id"),
    "awards.id": ("award_id", "id"), "last_known_institutions.id": ("institution_id", "id"),
    "is_oa": ("is_oa", "bool"),
}


def indexed_predicate(field, kind, value):
    """Parse quoted literals and set logic before wrapping individual nested clauses."""
    if value.startswith("!") and value != "!null":
        return {"bool": {"must_not": [indexed_predicate(field, kind, value[1:])]}}
    operators = (("+", "must"), ("|", "should")) if kind not in ("int", "float", "date", "bool") else (("|", "should"),)
    for separator, operation in operators:
        values = split_values(value, separator)
        if len(values) > 100:
            raise ValueError("maximum 100 set values")
        if len(values) > 1:
            result = {operation: [indexed_predicate(field, kind, v) for v in values]}
            if operation == "should":
                result["minimum_should_match"] = 1
            return {"bool": result}
    nested = field.startswith("authorships.")
    if value in ("null", "!null"):
        predicate = {"exists": {"field": field}}
        if nested:
            predicate = {"nested": {"path": "authorships", "query": predicate}}
        return {"bool": {"must_not": [predicate]}} if value == "null" else predicate
    literal = unquote(value)
    if kind in ("int", "float", "date"):
        predicate = local.clause(field, kind, literal)
    else:
        if kind == "str":
            literal = literal.lower()
        predicate = {"term": {field: local.convert(literal, kind)}}
    return {"nested": {"path": "authorships", "query": predicate}} if nested else predicate


def can_route(entity, params, filters):
    if not filters or any(k in params for k in ("group_by", "sample")):
        return False
    allowed_params = {"filter", "sort", "select", "page", "per_page", "cursor", "api_key", "mailto"}
    if set(params) - allowed_params:
        return False
    for key, value in filters:
        key = field_name(entity, key)
        if key.startswith(("from_", "to_")):
            key = key.split("_", 1)[1]
        if key not in PROJECTED or key not in fields(entity) or key == "id":
            return False
        if key in ("title", "display_name") and len(value) > 8191:
            return False
    return True


def text_query(text, fields, exact=False):
    tokens = re.findall(r'"(?:[^"\\]|\\.)*"(?:~\d+)?|[()]|[^\s()]+', text)
    if not tokens or len(tokens) > 400:
        raise ValueError("search needs 1-400 terms/operators")
    position = 0

    def leaf(token):
        fs = list(fields) if exact else [f + suffix for f in fields for suffix in (".en", ".zh")]
        if token.startswith('"'):
            match = re.fullmatch(r'("(?:[^"\\]|\\.)*")(?:~(\d+))?', token)
            if not match:
                raise ValueError("invalid quoted phrase")
            return {"multi_match": {"query": json.loads(match[1]), "fields": list(fields),
                                    "type": "phrase", "slop": int(match[2] or 0)}}
        if '"' in token:
            raise ValueError("unclosed quote in search")
        fuzzy = re.fullmatch(r"(.{3,})~([012])", token)
        if "*" in token or "?" in token:
            if not exact:
                raise ValueError("wildcards require an exact search parameter")
            if not re.match(r"[^*?]{3,}[*?]", token):
                raise ValueError("wildcard needs at least 3 preceding characters")
            return {"bool": {"should": [{"wildcard": {f: {"value": token.lower(), "case_insensitive": True}}} for f in fields], "minimum_should_match": 1}}
        body = dict(query=fuzzy[1] if fuzzy else token, fields=fs, type="best_fields", operator="and")
        if fuzzy:
            body["fuzziness"] = int(fuzzy[2])
        return {"multi_match": body}

    def atom(depth=0):
        nonlocal position
        if depth > 32 or position >= len(tokens):
            raise ValueError("invalid or excessively nested Boolean search")
        token = tokens[position]
        position += 1
        if token == "NOT":
            return {"bool": {"must_not": [atom(depth + 1)]}}
        if token == "(":
            result = disjunction(depth + 1)
            if position >= len(tokens) or tokens[position] != ")":
                raise ValueError("unclosed Boolean parentheses")
            position += 1
            return result
        if token in ("AND", "OR", ")"):
            raise ValueError("unexpected Boolean operator")
        return leaf(token)

    def conjunction(depth):
        nonlocal position
        parts = [atom(depth)]
        while position < len(tokens) and tokens[position] not in ("OR", ")"):
            if tokens[position] == "AND":
                position += 1
            parts.append(atom(depth))
        return parts[0] if len(parts) == 1 else {"bool": {"must": parts}}

    def disjunction(depth):
        nonlocal position
        parts = [conjunction(depth)]
        while position < len(tokens) and tokens[position] == "OR":
            position += 1
            parts.append(conjunction(depth))
        return parts[0] if len(parts) == 1 else {"bool": {"should": parts, "minimum_should_match": 1}}

    result = disjunction(0)
    if position != len(tokens):
        raise ValueError("unmatched Boolean parentheses")
    return result


def build(entity, params, filters):
    must, predicates = [], []
    searches = [(k, v) for k, v in params.items() if k in SCOPES]
    for key, value in filters:
        if key in FILTER_SEARCH:
            searches.append((FILTER_SEARCH[key], value))
            continue
        key = field_name(entity, key)
        if key.startswith(("from_", "to_")):
            prefix, key = key.split("_", 1)
            value = (">=" if prefix == "from" else "<=") + value
        if key not in PROJECTED or key not in fields(entity):
            raise ValueError("filter is not yet indexed for combined text search: " + key)
        field, kind = PROJECTED[key]
        if kind in ("int", "float"):
            value = re.sub(r"^(\d+)-(\d+)$", r"\1..\2", value)
        if kind == "date":
            value = re.sub(r"^(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})$", r"\1..\2", value)
        predicates.append(indexed_predicate(field, kind, value))
    for scope, value in searches:
        fs = SCOPES[scope] if entity == "works" else ("name", "aliases", "description")
        must.append(text_query(value, fs, scope.endswith(".exact")))
    sorts = []
    allowed = {"id": "id", "publication_year": "publication_year", "publication_date": "publication_date",
               "cited_by_count": "cited_by_count", "works_count": "works_count", "relevance_score": "_score", "display_name": "name_exact"}
    for part in params.get("sort", "relevance_score:desc" if searches else "id:asc").split(","):
        key, sep, direction = part.partition(":")
        if key not in allowed or (sep and direction not in ("asc", "desc")):
            raise ValueError("unsupported search sort: " + part)
        sorts.append({allowed[key]: {"order": direction if sep else "asc"}})
    if not any("id" in s for s in sorts):
        sorts.append({"id": {"order": "asc"}})
    return {"query": {"bool": {"must": must, "filter": predicates}}, "sort": sorts}
