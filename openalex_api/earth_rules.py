from .config import RESOURCE_ROOT
"""Explainable Earth-related membership over frozen OpenAlex metadata."""
import json
import re
from pathlib import Path

from .backend import sql_list, sql_string

VERSION = 2
POLICY_PATH = RESOURCE_ROOT / "earth-sciences-rules.json"
EVIDENCE_SETTINGS = " SETTINGS short_circuit_function_evaluation='force_enable'"


def tail(value):
    return str(value or "").rstrip("/").rsplit("/", 1)[-1]


def unicode_regex(pattern, capture=False):
    """RE2's \\b/\\w are ASCII; keep Python's Unicode word boundaries in SQL."""
    if not pattern.startswith(r"(?i)\b") or not pattern.endswith(r"\b"):
        raise ValueError("Earth text patterns require outer word boundaries")
    body = pattern[len(r"(?i)\b"):-len(r"\b")].replace(r"\w", r"[\p{L}\p{N}_]")
    group = "(" + body + ")" if capture else "(?:" + body + ")"
    return r"(?i)(?:^|[^\p{L}\p{N}_])" + group + r"(?:$|[^\p{L}\p{N}_])"


def text_match_sql(expression, pattern):
    # ASCII text has identical word boundaries and avoids the Unicode regex cost.
    return "if(length("+expression+")=lengthUTF8("+expression+"),match("+expression+","+sql_string(pattern)+"),match("+expression+","+sql_string(unicode_regex(pattern))+"))"


def policy():
    value = json.loads(POLICY_PATH.read_text())
    if value.get("version") != VERSION or value.get("label") != "earth_sciences":
        raise ValueError("unsupported Earth classification policy")
    for key in ("topic_pattern", "title_pattern", "abstract_token_pattern", "context_pattern", "journal_name_pattern", "concept_pattern"):
        re.compile(value[key])
    if any(not 0 <= value[k] <= 1 for k in ("concept_min_score", "keyword_min_score", "author_min_topic_score")) or min(value["author_min_core_works"], value["abstract_min_distinct_terms"], value["journal_profile_min_works"]) < 1:
        raise ValueError("invalid Earth evidence thresholds")
    return value


def compile_rules(p, taxonomy, concepts, journals, legacy_topics=()):
    legacy = {t["id"] for t in legacy_topics}
    topics = []
    for t in taxonomy.get("topics", []):
        reasons = []
        field, subfield = tail((t.get("field") or {}).get("id")), tail((t.get("subfield") or {}).get("id"))
        if field in p["fields"]: reasons.append("field:" + field)
        if subfield in p["subfields"]: reasons.append("subfield:" + subfield)
        if re.search(p["topic_pattern"], t.get("display_name", "")): reasons.append("topic_name")
        if tail(t["id"]) in legacy: reasons.append("legacy_topic")
        if reasons:
            topics.append(dict(id=tail(t["id"]), name=t.get("display_name", ""),
                               domain=tail((t.get("domain") or {}).get("id")), field=field, subfield=subfield, reasons=reasons))
    known = {t["id"] for t in topics}
    topics += [dict(t, reasons=["legacy_topic"]) for t in legacy_topics if t["id"] not in known]
    names = {n.casefold() for n in p["concept_roots"]}
    roots = {tail(c["id"]) for c in concepts if c["name"].casefold() in names}
    selected_concepts = [dict(id=tail(c["id"]), name=c["name"],
                              roots=sorted(roots & ({tail(c["id"])} | {tail(a) for a in c.get("ancestors") or []})))
                         for c in concepts if roots & ({tail(c["id"])} | {tail(a) for a in c.get("ancestors") or []})
                         or re.search(p["concept_pattern"],c["name"])]
    curated = {n.casefold() for n in p["journal_names"]}
    selected_journals = []
    for j in journals:
        if j.get("type") != "journal": continue
        name = j.get("display_name", "")
        ranked = sorted(j.get("topics") or [], key=lambda t: -t.get("count", 0))
        explicit = name.casefold() in curated
        profile = bool(ranked and tail((ranked[0].get("field") or {}).get("id")) in p["fields"]
                       and ranked[0].get("count", 0) >= p["journal_profile_min_works"])
        if explicit or (profile and re.search(p["journal_name_pattern"], name)):
            selected_journals.append(dict(id=tail(j["id"]), name=name, issn_l=j.get("issn_l"),
                                         issn=j.get("issn") or [], basis="curated_name" if explicit else "specialist_name_and_topic"))
    return dict(version=VERSION, policy=p,
                domains=[dict(id=tail(d["id"]), name=d["display_name"]) for d in taxonomy.get("domains", [])],
                fields=[dict(id=tail(d["id"]), name=d["display_name"]) for d in taxonomy.get("fields", []) if tail(d["id"]) in p["fields"]],
                subfields=[dict(id=tail(d["id"]), name=d["display_name"]) for d in taxonomy.get("subfields", []) if tail(d["id"]) in p["subfields"]],
                topics=sorted(topics, key=lambda t: t["id"]), concepts=sorted(selected_concepts, key=lambda t: t["id"]),
                journals=sorted(selected_journals, key=lambda t: t["id"]))


def classify(record, rules, seed_authors=()):
    """Independent Python audit of the database implementation; scores aren't probabilities."""
    p = rules["policy"]
    ts = list(record.get("topics") or []) + ([record["primary_topic"]] if record.get("primary_topic") else [])
    fields = sorted({int(tail((t.get("field") or {}).get("id"))) for t in ts if tail((t.get("field") or {}).get("id")).isdigit()})
    subfields = {tail((t.get("subfield") or {}).get("id")) for t in ts}
    reasons = {"field:" + str(f) for f in fields if str(f) in p["fields"]}
    core = any(tail((t.get("field") or {}).get("id")) in p["fields"] and (t.get("score") or 0) >= p["author_min_topic_score"] for t in ts)
    reasons |= {"subfield:" + s for s in subfields & set(p["subfields"])}
    topic_ids = {t["id"] for t in rules["topics"]}
    reasons |= {"topic:" + tail(t.get("id")) for t in ts if tail(t.get("id")) in topic_ids}
    concept_ids = {c["id"] for c in rules["concepts"]}
    reasons |= {"concept:" + tail(c.get("id")) for c in record.get("concepts") or []
                if tail(c.get("id")) in concept_ids and (c.get("score") or 0) >= p["concept_min_score"]}
    sources = [(record.get("primary_location") or {}).get("source") or {}]
    sources += [l.get("source") or {} for l in record.get("locations") or [] if l.get("is_published") or l.get("version") == "publishedVersion"]
    journal_ids = {j["id"] for j in rules["journals"]}
    journal_hits = {"journal:" + tail(s.get("id")) for s in sources if tail(s.get("id")) in journal_ids}
    reasons |= journal_hits
    title = record.get("title") or record.get("display_name") or ""
    keywords = [k.get("display_name", "") for k in record.get("keywords") or [] if (k.get("score") or 0) >= p["keyword_min_score"]]
    tokens = " ".join((record.get("abstract_inverted_index") or {}).keys())
    if re.search(p["title_pattern"],title): reasons.add("text:title")
    if any(re.search(p["title_pattern"],k) for k in keywords): reasons.add("text:keywords")
    abstract_terms = set(re.findall(p["abstract_token_pattern"], tokens.lower()))
    if len(abstract_terms) >= p["abstract_min_distinct_terms"]: reasons.add("text:abstract")
    context = bool(abstract_terms or re.search(p["context_pattern"], " ".join([title, " ".join(keywords), tokens])))
    authors = sorted({tail((a.get("author") or {}).get("id")) for a in record.get("authorships") or []
                      if re.fullmatch(r"A[0-9]+", tail((a.get("author") or {}).get("id")))})
    candidate = not reasons and context
    if candidate: reasons |= {"author:" + a for a in set(authors) & set(seed_authors)}
    return dict(field_ids=fields, reasons=sorted(reasons), author_seed=int(core or bool(journal_hits)),
                author_candidate=int(candidate), author_ids=authors if core or journal_hits or candidate else [],
                earth_related=bool(reasons), owner="earth_sciences" if reasons else "field_" + str(min(fields)) if fields else "unclassified")


def evidence_sql(rules):
    """One JSON pass produces compact facts; author support is applied after freezing seeds."""
    p = rules["policy"]
    values = lambda xs: sql_list(list(xs) or ["__no_match__"])
    title_match = text_match_sql("er_title",p["title_pattern"])
    keyword_match = text_match_sql("k",p["title_pattern"])
    context_match = text_match_sql("concat(er_title,' ',er_keywords,' ',er_abstract)",p["context_pattern"])
    token_pattern = sql_string(unicode_regex(p["abstract_token_pattern"], capture=True))
    fields, subfields = values(p["fields"]), values(p["subfields"])
    topics, concepts, journals = (values(r["id"] for r in rules[key]) for key in ("topics", "concepts", "journals"))
    return f"""WITH
arrayConcat(JSONExtractArrayRaw(raw,'topics'),[JSONExtractRaw(raw,'primary_topic')]) AS er_topics,
arraySort(arrayDistinct(arrayFilter(x -> x>0,arrayMap(t -> toUInt16OrZero(extract(JSONExtractString(t,'field','id'),'[0-9]+$')),er_topics)))) AS er_fields,
arrayFilter(t -> extract(JSONExtractString(t,'field','id'),'[^/]+$') IN {fields},er_topics) AS er_core,
arrayConcat([JSONExtractRaw(raw,'primary_location','source')],arrayMap(l -> JSONExtractRaw(l,'source'),arrayFilter(l -> JSONExtractBool(l,'is_published') OR JSONExtractString(l,'version')='publishedVersion',JSONExtractArrayRaw(raw,'locations')))) AS er_sources,
arrayMap(s -> concat('journal:',extract(JSONExtractString(s,'id'),'[^/]+$')),arrayFilter(s -> extract(JSONExtractString(s,'id'),'[^/]+$') IN {journals},er_sources)) AS er_journals,
if(empty(JSONExtractString(raw,'title')),JSONExtractString(raw,'display_name'),JSONExtractString(raw,'title')) AS er_title,
arrayMap(k -> JSONExtractString(k,'display_name'),arrayFilter(k -> JSONExtractFloat(k,'score') >= {p['keyword_min_score']},JSONExtractArrayRaw(raw,'keywords'))) AS er_keyword_names,
arrayStringConcat(er_keyword_names,' ') AS er_keywords,
lowerUTF8(arrayStringConcat(JSONExtractKeys(raw,'abstract_inverted_index'),' ')) AS er_abstract,
arrayDistinct(if(length(er_abstract)=lengthUTF8(er_abstract),extractAll(er_abstract,{sql_string(p['abstract_token_pattern'])}),extractAll(replaceRegexpAll(er_abstract,'[^\\\\p{{L}}\\\\p{{N}}_]','  '),{token_pattern}))) AS er_terms,
arraySort(arrayDistinct(arrayConcat(
arrayMap(t -> concat('field:',extract(JSONExtractString(t,'field','id'),'[^/]+$')),er_core),
arrayMap(t -> concat('subfield:',extract(JSONExtractString(t,'subfield','id'),'[^/]+$')),arrayFilter(t -> extract(JSONExtractString(t,'subfield','id'),'[^/]+$') IN {subfields},er_topics)),
arrayMap(t -> concat('topic:',extract(JSONExtractString(t,'id'),'[^/]+$')),arrayFilter(t -> extract(JSONExtractString(t,'id'),'[^/]+$') IN {topics},er_topics)),
arrayMap(c -> concat('concept:',extract(JSONExtractString(c,'id'),'[^/]+$')),arrayFilter(c -> extract(JSONExtractString(c,'id'),'[^/]+$') IN {concepts} AND JSONExtractFloat(c,'score') >= {p['concept_min_score']},JSONExtractArrayRaw(raw,'concepts'))),
er_journals,
if({title_match},['text:title'],[]),
if(arrayExists(k -> {keyword_match},er_keyword_names),['text:keywords'],[]),
if(length(er_terms)>={p['abstract_min_distinct_terms']},['text:abstract'],[])))) AS er_reasons,
arrayExists(t -> JSONExtractFloat(t,'score') >= {p['author_min_topic_score']},er_core) OR notEmpty(er_journals) AS er_seed,
empty(er_reasons) AND (notEmpty(er_terms) OR {context_match}) AS er_candidate,
arraySort(arrayDistinct(arrayFilter(a -> match(a,'^A[0-9]+$'),arrayMap(a -> extract(JSONExtractString(a,'author','id'),'[^/]+$'),JSONExtractArrayRaw(raw,'authorships'))))) AS er_authors
SELECT id,version,digest AS raw_digest,er_fields AS field_ids,er_reasons AS reasons,
if(er_seed OR er_candidate,er_authors,[]) AS author_ids,toUInt8(er_seed) AS author_seed,toUInt8(er_candidate) AS author_candidate"""
