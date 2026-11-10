"""Replace proven equivalent filters; keep other predicates in the legacy compiler."""
from .manticore_engine import NUMBERS
from .manticore_formats import FILTERS, numeric


def exact(entity, node):
    if "exists" in node:
        field = node["exists"]["field"]
        if field in NUMBERS:
            return {"equals": {"has_" + field: 1}}
    for operator in ("term", "range"):
        if operator not in node:
            continue
        field, value = next(iter(node[operator].items()))
        if operator == "term" and isinstance(value, dict):
            value = value["value"]
        if field in NUMBERS and NUMBERS[field] != "float":
            clause = {"equals": {field: int(value)}} if operator == "term" else {"range": {field: value}}
            return {"bool": {"must": [{"equals": {"has_" + field: 1}}, clause]}}
        if operator == "term" and (field in FILTERS or field == "id"):
            name, prefix = FILTERS[field][:2] if field in FILTERS else ("id", {"works": "W", "topics": "T", "sources": "S", "authors": "A", "institutions": "I"}[entity])
            if field in FILTERS and FILTERS[field][2] == "multi64":
                name = "any(" + name + ")"
            try:
                return {"equals": {name: numeric(value, prefix)}}
            except ValueError:
                return None
    if "bool" in node:
        spec, result = node["bool"], {}
        required = spec.get("minimum_should_match", 0 if spec.get("must") or spec.get("filter") else 1)
        if required not in (0, 1):
            return None
        for kind in ("must", "filter", "must_not", "should"):
            if kind == "should" and not required:
                continue
            children = spec.get(kind, [])
            children = children if isinstance(children, list) else [children]
            converted = [exact(entity, child) for child in children]
            if any(child is None for child in converted):
                return None
            if converted:
                result.setdefault("must" if kind == "filter" else kind, []).extend(converted)
        if "should" in result and "must" in result:
            result["must"].append({"bool": {"should": result.pop("should")}})
        if result:
            return {"bool": result}
    return None


def split(entity, node):
    converted = exact(entity, node)
    if converted is not None:
        return {"match_all": {}}, [converted]
    if "bool" not in node:
        return node, []
    spec, filters = dict(node["bool"]), []
    # Keep optional SHOULD semantics unchanged when removing a required conjunct.
    if spec.get("should") and not spec.get("minimum_should_match", 0 if spec.get("must") or spec.get("filter") else 1):
        return node, []
    for kind in ("must", "filter"):
        if kind not in spec:
            continue
        remaining = []
        children = spec[kind] if isinstance(spec[kind], list) else [spec[kind]]
        for child in children:
            residual, extra = split(entity, child)
            filters.extend(extra)
            remaining.append(residual)
        spec[kind] = remaining
    return {"bool": spec}, filters
