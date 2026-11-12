"""Small source directories use metadata; work filters reuse existing projections."""
from .backend import sql_string

ENTITIES = ("domains", "fields", "subfields", "countries", "continents", "languages",
            "licenses", "institution-types", "source-types", "work-types", "sdgs")
WORK_FILTERS = {"domains": "domain", "fields": "field", "subfields": "subfield",
                "countries": "country", "languages": "language", "licenses": "license",
                "work-types": "type"}


def name_terms(params, filters):
    from .official import unquote
    from .official_text import SCOPES, FILTER_SEARCH
    unsupported = (set(params) & set(SCOPES)) - {"search"}
    unsupported |= {k for k, _ in filters if k in FILTER_SEARCH and k != "display_name.search"}
    if unsupported:
        raise ValueError("small directories support literal name search only")
    terms = [params.get("search", "")] + [unquote(v) for k, v in filters if k == "display_name.search"]
    if any(len(term) > 2000 for term in terms):
        raise ValueError("directory search exceeds 2000 characters")
    return [term.strip() for term in terms if term.strip()]


def name_predicate(term):
    return "positionCaseInsensitiveUTF8(JSONExtractString(raw,'display_name')," + sql_string(term) + ")>0"


def capabilities():
    return dict(entities=list(ENTITIES), work_filters=WORK_FILTERS,
                search="case-insensitive literal display_name substring; no text index required",
                scope="imported source metadata; work navigation uses published work search coverage")
