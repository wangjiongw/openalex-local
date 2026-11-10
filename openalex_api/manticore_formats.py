"""Explicit physical layouts; legacy files remain immutable and readable."""
import hashlib
import os

import orjson

from .config import ROOT
from .manticore_engine import NUMBERS, STRINGS, TEXT_FIELDS, checked_name, project

LAYOUT = "native_v2"
FILTERS = {
    "topic_id": ("filter_topic_ids", "T", "multi64"),
    "source_id": ("filter_source_ids", "S", "multi64"),
    "primary_topic_id": ("filter_primary_topic", "T", "bigint"),
    "primary_source_id": ("filter_primary_source", "S", "bigint"),
}


def engine_env():
    directory = ROOT / "var/engines/manticore-29.9.0/usr/share/manticore/modules"
    for name in ("secondary", "columnar"):
        if not (directory / ("lib_manticore_" + name + ".so")).is_file():
            raise RuntimeError("missing Manticore module: " + name)
    return os.environ | {"MANTICORE_MODULES": str(directory)}


def create(client, table):
    columns = [f"{f} text" for f in TEXT_FIELDS] + [f"{f} {t}" for f, t in NUMBERS.items()]
    columns += [f"{f} string" for f in STRINGS] + ["attrs json secondary_index='1'"]
    columns += [name + " " + kind for name, _, kind in FILTERS.values()]
    columns += ["has_" + name + " integer" for name in NUMBERS]
    client.sql(f"CREATE TABLE {checked_name(table)} ({','.join(columns)}) "
               "morphology='stem_en' index_exact_words='1' "
               "charset_table='non_cjk' ngram_len='1' ngram_chars='cjk' rt_mem_limit='256M' optimize_cutoff='8'")


def numeric(value, prefix):
    if not isinstance(value, str) or not value.startswith(prefix) or not value[1:].isascii() or not value[1:].isdigit():
        raise ValueError("invalid projected identifier")
    number = int(value[1:])
    if not 0 < number < 2**63:
        raise ValueError("projected identifier exceeds signed 64-bit range")
    return number


def project_native(entity, row, group, reasons):
    key, doc = project(entity, row, group, reasons)
    attrs = doc["attrs"]
    for field, (name, prefix, kind) in FILTERS.items():
        value = attrs.get(field)
        doc[name] = sorted({numeric(v, prefix) for v in value or []}) if kind == "multi64" else numeric(value, prefix) if value else 0
    for name in NUMBERS:
        doc["has_" + name] = int(attrs["present"].get(name) == 1)
    doc.pop("projection_hash")
    doc["projection_hash"] = hashlib.sha256(orjson.dumps(doc, option=orjson.OPT_SORT_KEYS)).hexdigest()
    return key, doc


def check_indexes(client, name):
    state = {r["Variable_name"]: r["Value"] for r in client.rows("SHOW TABLE " + checked_name(name) + " STATUS")}
    if int(state["ram_chunk"]) or int(state["disk_chunks"]) > 8:
        raise RuntimeError("native layout has unsealed RAM or excessive disk chunks: " + name)
    indexes = client.rows("SHOW TABLE " + name + " INDEXES")
    enabled = {r["Name"] for r in indexes if str(r["Enabled"]) == "1" and float(r["Percent"]) == 100}
    required = {"id", "publication_year", "is_oa", "has_publication_year", "has_is_oa"}
    required.update(v[0] for v in FILTERS.values())
    if not required <= enabled:
        raise RuntimeError("native secondary indexes missing: " + str(sorted(required-enabled)))
    return dict(disk_chunks=int(state["disk_chunks"]), indexes=indexes)
