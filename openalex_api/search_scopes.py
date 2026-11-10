"""Frozen scope membership and disjoint, reusable search blocks."""
import json
import re

from .config import ENTITIES, generation_path, read_generation
from .backend import sql_string, sql_list

VERSION = 2
BLOCK_ROWS = 5_000_000
EARTH = "earth_sciences"
EXTRA = re.compile(r"remote sensing|geographic information systems|geospatial|geodes|soil|geotechnical|seism|earthquake|tsunami|geothermal|hydrolog|watershed|biogeochem|climate change|climate adaptation", re.I)


def catalog_path(g):
    return generation_path(g) / "search-catalog.json"


def topic_rules(topics):
    selected = []
    for t in topics:
        field = str((t.get("field") or {}).get("id", "")).rsplit("/", 1)[-1]
        subfield = str((t.get("subfield") or {}).get("id", "")).rsplit("/", 1)[-1]
        if field in ("19", "23") or subfield == "1111" or EXTRA.search(t.get("display_name", "")):
            selected.append(dict(id=t["id"].rsplit("/", 1)[-1], name=t.get("display_name", "")))
    return sorted(selected, key=lambda x: x["id"])


def classify(record, rules):
    topics = list(record.get("topics") or [])
    if record.get("primary_topic"):
        topics.append(record["primary_topic"])
    fields = sorted({int(str((t.get("field") or {}).get("id", "")).rsplit("/", 1)[-1])
                     for t in topics if str((t.get("field") or {}).get("id", "")).rsplit("/", 1)[-1].isdigit()})
    earth = any(str(t.get("id", "")).rsplit("/", 1)[-1] in rules
                or str((t.get("field") or {}).get("id", "")).rsplit("/", 1)[-1] in ("19", "23")
                or str((t.get("subfield") or {}).get("id", "")).rsplit("/", 1)[-1] == "1111" for t in topics)
    return fields, EARTH if earth else "field_" + str(min(fields)) if fields else "unclassified"


def load(g, complete=True):
    value = json.loads(catalog_path(g).read_text())
    from .search_versions import require_metadata
    if value.get("metadata_signature") != require_metadata(g)["signature"] or value.get("rule_version") not in (1, VERSION):
        raise RuntimeError("search catalog identity changed")
    if complete and (value.get("state") != "complete" or value.get("rule_version") != VERSION):
        raise RuntimeError("search catalog is not complete")
    return value


def build_catalog(g):
    from .earth_catalog import build_catalog as build
    return build(g)


def selection(g, manifest, entity, prefix=""):
    value = manifest.get("selection")
    if not value:
        return "1"
    if entity != "works":
        return prefix + "id>" + sql_string(value.get("low", "")) + " AND " + prefix + "id<=" + sql_string(value["high"])
    catalog = load(g)
    if catalog["signature"] != manifest["catalog_signature"]:
        raise RuntimeError("build scope differs from catalog")
    where = "owner=" + sql_string(value["owner"])
    if value.get("low"):
        where += " AND id>" + sql_string(value["low"])
    if value.get("high"):
        where += " AND id<=" + sql_string(value["high"])
    return prefix + "id IN (SELECT id FROM " + catalog["table"] + " FINAL WHERE " + where + ")"


def source_rows(b, g, entity, after, size=1000, selection_value=None):
    from .official import source_table
    cache = getattr(b, "scope_sources", None)
    if not isinstance(cache, dict):
        cache = b.scope_sources = {}
    key = (g, entity)
    if key not in cache:
        cache[key] = source_table(b, read_generation(g), entity)
    table = cache[key]
    if selection_value:
        if (g, "catalog") not in cache:
            cache[g,"catalog"] = load(g)
        catalog = cache[g,"catalog"] if entity == "works" else None
        where = "owner=" + sql_string(selection_value["owner"]) if catalog else "entity=" + sql_string(entity)
        if selection_value.get("low"):
            where += " AND id>" + sql_string(selection_value["low"])
        if selection_value.get("high"):
            where += " AND id<=" + sql_string(selection_value["high"])
        member_table = catalog["table"] + " FINAL" if catalog else table
        ids = b.ch(f"SELECT id FROM {member_table} WHERE {where} AND id>{sql_string(after)} ORDER BY id LIMIT {int(size)} FORMAT JSONEachRow")
        if not ids:
            return []
        rows = b.ch(f"SELECT id,version,raw FROM {table} WHERE entity={sql_string(entity)} AND id IN {sql_list([r['id'] for r in ids])} ORDER BY id FORMAT JSONEachRow")
        if len(rows) != len(ids):
            raise RuntimeError("scope source is missing records")
        return rows
    return b.ch(f"SELECT id,version,raw FROM {table} WHERE entity={sql_string(entity)} AND id>{sql_string(after)} ORDER BY id LIMIT {int(size)} FORMAT JSONEachRow")


def validate_scope(scope):
    if scope in ("available",EARTH,"unclassified"):
        return
    if re.fullmatch(r"field_[0-9]+",scope) and 11<=int(scope[6:])<=36:
        return
    raise ValueError("unknown search_scope")


def scope_info(meta, entity="works", scope="available"):
    manifest = meta.get("_search_build") or {}
    validate_scope(scope)
    if scope != "available" and entity != "works":
        raise ValueError("search_scope is supported for works only")
    if manifest.get("incremental"):
        ready = manifest.get("ready_scopes", [])
        if scope != "available" and scope not in ready:
            from fastapi import HTTPException
            raise HTTPException(503, detail=dict(code="search_not_ready", search_scope=scope, message="Requested scope is not published."))
        selected = [s for s in manifest["segments"] if s["entity"] == entity
                    and (scope not in (EARTH,"unclassified") or s.get("owner") == scope)]
        indexes = manifest.get("scope_views",{}).get(scope) or manifest.get("views",{}).get(entity) or ",".join(s["index"] for s in selected)
        total = sum(s["count"] for s in selected)
    else:
        if scope != "available":
            from fastapi import HTTPException
            raise HTTPException(503, detail=dict(code="search_not_ready", search_scope=scope))
        indexes = manifest.get("indexes", {}).get(entity, "")
        total = manifest.get("entities", {}).get(entity, {}).get("count")
    scope_filter = {"term": {"scope_field_ids": "fields/" + scope[6:]}} if scope.startswith("field_") else None
    return dict(scope_filter=scope_filter, search_scope=scope, published_scopes=manifest.get("ready_scopes", []),
                indexed_entities=total, global_search_complete=bool(manifest.get("global_complete", not manifest.get("incremental") and bool(manifest))), indexes=indexes)


def all_indexes(manifest):
    if manifest.get("incremental"):
        return [s["index"] for s in manifest["segments"]]
    return list(manifest["indexes"].values())


def next_batch(g):
    from .search_versions import active
    catalog = load(g)
    previous = active(g)
    done = set(previous.get("ready_scopes", [])) if previous else set()
    for row in catalog["owners"]:
        if row["owner"] not in done:
            return row["owner"], "works", row["count"]
    for entity in ENTITIES:
        if entity != "works" and (not previous or entity not in previous.get("indexes", {})):
            return entity, entity, read_generation(g)["expected"][entity]["records"]
    return None
