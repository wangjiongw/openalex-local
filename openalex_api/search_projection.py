"""Versioned search documents; the frozen metadata contract stays in model.py."""
import hashlib
import unicodedata
import orjson
from . import model

VERSION = 1


def latin_alias(value):
    """Preserve non-Latin scripts while adding an unaccented Latin name variant."""
    output = []
    for char in unicodedata.normalize("NFD", value):
        if unicodedata.combining(char) and output and "LATIN" in unicodedata.name(output[-1], ""):
            continue
        output.append(char)
    return unicodedata.normalize("NFC", "".join(output))


def project(entity, record):
    document = model.project(entity, record)
    document["name_literal"] = (record.get("title") or record.get("display_name") or "").lower()
    if entity == "works":
        from .search_scopes import classify
        document["scope_field_ids"] = ["fields/"+str(n) for n in classify(record,set())[0]]
        for authorship in document["authorships"]:
            names = authorship["author_name"]
            authorship["author_name"] = list(dict.fromkeys(names + [latin_alias(n) for n in names]))
    else:
        names = [document["name"], *document["aliases"]]
        document["aliases"] = list(dict.fromkeys(document["aliases"] + [latin_alias(n) for n in names if latin_alias(n) != n]))
    document["projection_hash"] = hashlib.sha256(
        orjson.dumps(document, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()
    return document


def mapping(shards=1):
    result = model.mapping(shards)
    result["mappings"]["properties"]["projection_hash"] = dict(
        type="keyword", index=False, doc_values=True, store=True
    )
    result["mappings"]["properties"]["scope_field_ids"] = dict(type="keyword")
    result["mappings"]["properties"]["name_literal"] = dict(type="keyword", ignore_above=8191)
    result["settings"].update({
        "index.store.type": "niofs", "index.translog.durability": "request",
        "index.merge.scheduler.max_thread_count": 1,
    })
    return result
