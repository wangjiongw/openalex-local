"""Translate the existing validated query tree; filters run before LIMIT."""
import re

from .backend import sql_string
from .manticore_engine import NUMBERS, TEXT_FIELDS, normalize
from .query import FIELDS

ATTRS = {f for f, _ in FIELDS.values()} | {"name_literal", "primary_topic_id", "primary_source_id", "earth_group", "scope_field_ids"}
ARRAYS = {"country", "source_id", "issn", "publisher_id", "topic_id", "keyword_id", "concept_id", "funder_id",
          "award_id", "domain_id", "field_id", "subfield_id", "license", "scope_field_ids", "institution_id"}
NESTED_ARRAYS = {"institution_id", "ror", "country"}


def field_name(field):
    field = field.split("^")[0]
    if field.endswith(".en"):
        field = field[:-3] + "_en"
    elif field.endswith(".zh"):
        field = field[:-3]
    field = field.removeprefix("authorships.")
    if field not in TEXT_FIELDS:
        raise ValueError("text field is not supported by Manticore: " + field)
    return field


def literal_text(value):
    return re.sub(r'([\\()|\-!@~"&/^$=<>{}*?])', r'\\\1', normalize(value))


def text_clause(spec):
    if spec.get("fuzziness") is not None or spec.get("slop", 0):
        raise ValueError("fuzzy/proximity operators are not enabled in this search release")
    fields = list(dict.fromkeys(field_name(f) for f in spec["fields"]))
    words = re.findall(r"[^\W_]+", normalize(spec["query"]))
    if not words:
        raise ValueError("search requires nonempty text")
    choices = []
    for stemmed in (True, False):
        selected = [f for f in fields if f.endswith("_en") == stemmed]
        if not selected:
            continue
        text = " ".join(("" if stemmed else "=") + w for w in words)
        if spec.get("type") == "phrase":
            text = '"' + text + '"'
        choices.append("(@(" + ",".join(selected) + ") " + text + ")")
    return "(" + " | ".join(choices) + ")"


def name_regex(value, phrase=False):
    tokens = re.findall(r"[^\W_]+", normalize(value))
    if not tokens:
        raise ValueError("author search needs letters or numbers")
    boundary = r"[^\p{L}\p{N}]"
    if phrase:
        return [r"(^|" + boundary + ")" + (boundary + "+").join(re.escape(t) for t in tokens) + r"($|" + boundary + ")"]
    return [r"(^|" + boundary + ")" + re.escape(t) + r"($|" + boundary + ")" for t in tokens]


def conjunction(parts, op="AND"):
    parts = [p for p in parts if p]
    return "(" + (" " + op + " ").join(parts) + ")" if parts else ""


def attribute(field, nested=False):
    if field not in ATTRS:
        raise ValueError("filter is not indexed: " + field)
    if nested:
        if not field.startswith("authorships."):
            raise ValueError("invalid authorship field")
        key = field.split(".", 1)[1]
        return "x." + key, "x.present." + key
    if field.startswith("authorships."):
        raise ValueError("authorship filters require a nested predicate")
    return field if field in NUMBERS else "attrs." + field, "attrs.present." + field


def value_sql(value):
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, (int, float)):
        return str(value)
    return sql_string(value)


def required_text(node):
    if "multi_match" in node:
        return text_clause(node["multi_match"])
    if "bool" in node:
        spec = node["bool"]
        return " ".join(filter(None, (required_text(c) for k in ("must", "filter") for c in spec.get(k, []))))
    return ""


def compile_node(node, nested=False):
    if "match_all" in node:
        return "", "1"
    if "multi_match" in node:
        spec = node["multi_match"]
        if not nested:
            return text_clause(spec), ""
        if spec.get("fuzziness") is not None or spec.get("slop", 0):
            raise ValueError("fuzzy/proximity author matching is not enabled")
        choices = []
        for field in spec["fields"]:
            key = field_name(field)
            if key not in ("author_name", "institution_name"):
                raise ValueError("unsupported nested text field")
            choices.append(conjunction([f"REGEX(x.{key}_text,{sql_string(p)})"
                                        for p in name_regex(spec["query"], spec.get("type") == "phrase")]))
        return "", conjunction(choices, "OR")
    if "match_phrase" in node:
        key, value = next(iter(node["match_phrase"].items()))
        value = value if isinstance(value, dict) else {"query": value}
        return compile_node({"multi_match": dict(value, fields=[key], type="phrase")}, nested)
    if "nested" in node:
        if nested or node["nested"]["path"] != "authorships":
            raise ValueError("unsupported nested query")
        sub = node["nested"]["query"]
        _, predicate = compile_node(sub, True)
        return required_text(sub), f"ANY({predicate or '1'} FOR x IN attrs.authorships)"
    if "exists" in node:
        _, present = attribute(node["exists"]["field"], nested)
        return "", present + "=1"
    if "term" in node:
        key, value = next(iter(node["term"].items()))
        if isinstance(value, dict):
            value = value["value"]
        column, present = attribute(key, nested)
        short = key.split(".")[-1]
        if nested and short in NESTED_ARRAYS:
            # Scalar membership avoids nested ANY variable scopes in Manticore JSON expressions.
            pattern = r"(^|\x1f)" + re.escape(str(value)) + r"($|\x1f)"
            expression = f"REGEX(x.{short}_keys,{sql_string(pattern)})"
        elif key in ARRAYS:
            expression = f"ANY(v={value_sql(value)} FOR v IN {column})"
        else:
            expression = column + "=" + value_sql(value)
        return "", f"({present}=1 AND {expression})"
    if "range" in node:
        key, bounds = next(iter(node["range"].items()))
        column, present = attribute(key, nested)
        if key not in NUMBERS:
            raise ValueError("range filter is not indexed: " + key)
        operators = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<"}
        return "", conjunction([present + "=1"] + [column + operators[k] + value_sql(v) for k, v in bounds.items()])
    if "bool" in node:
        spec = node["bool"]
        text, predicates = [], []
        for key in ("must", "filter"):
            children = spec.get(key, [])
            if isinstance(children, dict):
                children = [children]
            for child in children:
                t, p = compile_node(child, nested)
                if t:
                    text.append(t)
                if p:
                    predicates.append(p)
        for child in spec.get("must_not", []):
            t, p = compile_node(child, nested)
            if t and p:
                raise ValueError("mixed text/filter negation is not supported")
            if t:
                text.append("!(" + t + ")")
            if p:
                predicates.append("NOT(" + p + ")")
        required = spec.get("minimum_should_match", 0 if spec.get("must") or spec.get("filter") else 1)
        if spec.get("should") and required:
            choices = [compile_node(child, nested) for child in spec["should"]]
            if all(t and not p for t, p in choices):
                text.append("(" + " | ".join(t for t, _ in choices) + ")")
            elif all(p and not t for t, p in choices):
                predicates.append(conjunction([p for _, p in choices], "OR"))
            else:
                raise ValueError("mixed text/filter OR is not supported")
        return " ".join(text), conjunction(predicates)
    raise ValueError("search operator is not enabled: " + next(iter(node), "empty"))


def request_body(body, tables, limit, offset=0, scroll=None):
    from .manticore_engine import checked_name
    if body.get("aggs") or body.get("aggregations"):
        raise ValueError("text aggregations are not enabled in this release")
    text, predicate = compile_node(body["query"])
    must, expressions = [], {}
    if text:
        must.append({"query_string": "(@universe =oaalltoken) " + text})
    if predicate and predicate != "1":
        expressions["oa_predicate"] = predicate
        must.append({"equals": {"oa_predicate": 1}})
    sorts = []
    for item in body.get("sort", [{"id": {"order": "asc"}}]):
        key, value = next(iter(item.items()))
        direction = value.get("order", "asc")
        if direction not in ("asc", "desc"):
            raise ValueError("invalid sort direction")
        if key == "id":
            key = "original_id"
        elif key in ("name_exact", "name_literal"):
            expressions["oa_name_sort"] = "TO_STRING(attrs." + key + ")"
            key = "oa_name_sort"
        elif key != "_score" and key not in NUMBERS:
            raise ValueError("sort is not indexed: " + key)
        sorts.append({key: direction})
    sorts.append({"id": "asc"})
    return dict(table=",".join(checked_name(t) for t in tables), query={"bool": {"must": must}} if must else {"match_all": {}},
                expressions=expressions, sort=sorts, limit=limit, offset=offset,
                track_scores=True, _source=["original_id", "group_name", "attrs"],
                options={"scroll": scroll or True, "cutoff": 0, "accurate_aggregation": 1,
                         "max_matches": max(1000, offset + limit),
                         "field_weights": {"name": 5, "name_en": 5, "keywords": 3, "keywords_en": 3, "abstract": 1, "abstract_en": 1}})
