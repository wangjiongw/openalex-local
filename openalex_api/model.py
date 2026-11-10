"""Lossless source storage; a deliberately bounded search projection."""

import re, unicodedata

PREFIX = re.compile(r"^https?://(?:api\.)?openalex\.org/", re.I)
NUMERIC = re.compile(r"^[WAI SFTCPG]\d+$".replace(" ", ""))


def canonical_id(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("missing or non-string entity ID")
    value = PREFIX.sub("", value.strip())
    if re.fullmatch(r"[waisftcpg]\d+", value, re.I):
        return value.upper()
    return value


def numeric_id(value):
    value = canonical_id(value)
    if not NUMERIC.fullmatch(value):
        return None
    n = int(value[1:])
    if n >= 2**64:
        raise ValueError("ID exceeds UInt64")
    return n


def doi(value):
    if not isinstance(value, str):
        return None
    value = re.sub(
        r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", value.strip(), flags=re.I
    ).lower()
    return value if re.match(r"^10\.\d{4,9}/\S+$", value) else None


def normalize_title(value):
    return " ".join(
        "".join(
            c if c.isalnum() else " "
            for c in unicodedata.normalize("NFKC", value or "").casefold()
        ).split()
    )


def abstract(index):
    if not index:
        return ""
    if not isinstance(index, dict):
        raise ValueError("abstract_inverted_index must be an object or null")
    positions = []
    for word, offsets in index.items():
        if not isinstance(offsets, list):
            raise ValueError("abstract offsets must be a list")
        for offset in offsets:
            if type(offset) is not int or offset < 0:
                raise ValueError("invalid abstract offset")
            positions.append((offset, word))
    return " ".join(w for _, w in sorted(positions))


def ids(items):
    return sorted(
        {
            canonical_id(x["id"] if isinstance(x, dict) else x)
            for x in items or []
            if x and (not isinstance(x, dict) or x.get("id"))
        }
    )


def external_ids(record):
    pairs = set()
    for scheme, v in (record.get("ids") or {}).items():
        if scheme == "openalex" or v is None:
            continue
        for item in v if isinstance(v, list) else [v]:
            if isinstance(item, (str, int)):
                pairs.add((scheme, str(item).strip().lower()))
    for scheme in ["doi", "orcid", "ror", "issn", "issn_l"]:
        v = record.get(scheme)
        for item in v if isinstance(v, list) else [v]:
            if item:
                pairs.add((scheme, str(item).strip().lower()))
    out = set()
    for scheme, value in pairs:
        if scheme == "doi":
            value = doi(value)
            if not value:
                continue
        if scheme in ["orcid", "ror", "pmid", "pmcid"]:
            value = value.rstrip("/").split("/")[-1]
        if scheme == "issn_l":
            scheme = "issn"
        out.add((scheme, value))
    return sorted(out)


def project(entity, r):
    key = canonical_id(r.get("id"))
    numeric_id(key)
    p = {
        "id": key,
        "name": r.get("title") or r.get("display_name") or "",
        "aliases": r.get("display_name_alternatives") or [],
        "name_exact": normalize_title(r.get("title") or r.get("display_name")),
        "type": r.get("type"),
        "cited_by_count": r.get("cited_by_count"),
        "works_count": r.get("works_count"),
        "country": r.get("country_code"),
        "external_ids": [f"{s}:{v}" for s, v in external_ids(r)],
    }
    if entity != "works":
        p["description"] = r.get("description") or ""
        p["institution_id"] = ids(r.get("last_known_institutions"))
        p["institution_name"] = [
            i.get("display_name", "") for i in r.get("last_known_institutions") or []
        ]
        return p
    p.update(
        {
            k: r.get(k)
            for k in [
                "publication_year",
                "publication_date",
                "language",
                "is_xpac",
                "is_retracted",
                "is_paratext",
                "has_fulltext",
                "fwci",
            ]
        }
    )
    p["abstract"] = abstract(r.get("abstract_inverted_index"))
    oa = r.get("open_access") or {}
    p.update(is_oa=oa.get("is_oa"), oa_status=oa.get("oa_status"))
    p["authorships"] = []
    for a in r.get("authorships") or []:
        author = a.get("author") or {}
        ins = a.get("institutions") or []
        p["authorships"].append(
            dict(
                author_id=canonical_id(author["id"]) if author.get("id") else None,
                author_name=list(
                    filter(None, [author.get("display_name"), a.get("raw_author_name")])
                ),
                orcid=(author.get("orcid") or "").rstrip("/").split("/")[-1].lower()
                or None,
                institution_id=ids(ins),
                institution_name=[i.get("display_name", "") for i in ins],
                ror=[
                    i["ror"].rstrip("/").split("/")[-1].lower()
                    for i in ins
                    if i.get("ror")
                ],
                country=a.get("countries")
                or [i["country_code"] for i in ins if i.get("country_code")],
                is_corresponding=a.get("is_corresponding"),
            )
        )
    p["country"] = sorted({c for a in p["authorships"] for c in a["country"]})
    locations = r.get("locations") or []
    sources = [x["source"] for x in locations if x.get("source")]
    p["source_id"] = ids(sources)
    p["source_name"] = [s.get("display_name", "") for s in sources]
    p["issn"] = sorted({v.lower() for s in sources for v in (s.get("issn") or [])})
    p["publisher_id"] = ids(
        [
            s["host_organization"]
            for s in sources
            if (s.get("host_organization") or "").split("/")[-1].startswith("P")
        ]
    )
    p["license"] = sorted({x["license"] for x in locations if x.get("license")})
    for field in ["topics", "keywords", "concepts", "funders", "awards"]:
        p[field.rstrip("s") + "_id"] = ids(r.get(field))
    for level in ["domain", "field", "subfield"]:
        p[level + "_id"] = ids(
            [t[level] for t in r.get("topics") or [] if t.get(level)]
        )
    return p


TEXT_FIELDS = [
    "name",
    "aliases",
    "abstract",
    "description",
    "source_name",
    "institution_name",
]
KEY_FIELDS = [
    "id",
    "name_exact",
    "type",
    "language",
    "country",
    "external_ids",
    "oa_status",
    "source_id",
    "issn",
    "publisher_id",
    "license",
    "topic_id",
    "keyword_id",
    "concept_id",
    "funder_id",
    "award_id",
    "domain_id",
    "field_id",
    "subfield_id",
    "institution_id",
]


def text_mapping():
    return {
        "type": "text",
        "analyzer": "standard",
        "fields": {
            "en": {"type": "text", "analyzer": "english"},
            "zh": {"type": "text", "analyzer": "cjk"},
        },
    }


def mapping(shards=1):
    props = {f: {"type": "keyword", "ignore_above": 8191} for f in KEY_FIELDS}
    props.update({f: text_mapping() for f in TEXT_FIELDS})
    props.update(
        {
            f: {"type": "long"}
            for f in ["publication_year", "cited_by_count", "works_count"]
        }
    )
    props.update(
        {
            f: {"type": "boolean"}
            for f in ["is_xpac", "is_retracted", "is_paratext", "is_oa", "has_fulltext"]
        }
    )
    props["fwci"] = {"type": "double"}
    props["publication_date"] = {"type": "date"}
    nested = {
        f: {"type": "keyword"}
        for f in ["author_id", "orcid", "institution_id", "ror", "country"]
    }
    nested.update({f: text_mapping() for f in ["author_name", "institution_name"]})
    nested["is_corresponding"] = {"type": "boolean"}
    props["authorships"] = {"type": "nested", "properties": nested}
    return {
        "settings": {
            "number_of_shards": shards,
            "number_of_replicas": 0,
            "refresh_interval": "30s",
            "index.mapping.nested_objects.limit": 100000,
        },
        "mappings": {
            "dynamic": "strict",
            "_source": {"enabled": False},
            "properties": props,
        },
    }
