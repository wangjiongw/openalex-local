"""Checkpointed evidence extraction and one-way author support, without changing source data."""
import json
import time
from pathlib import Path

from . import earth_rules
from .backend import Backend, database, sql_string, sql_list
from .config import generation_path, atomic_json, read_generation
from .proofs import digest, file_hash

BATCH_ROWS = 100_000
REPAIR_BATCH_ROWS = 10_000
ASCII_IMPLEMENTATION = "1ff02ca507835ff8817137a4010a392e4a914652d4e4d6f94581eca5bb29da47"


def freeze_rules(b, g, legacy):
    from .official import source_table
    meta = read_generation(g)
    p = earth_rules.policy()
    taxonomy = {e: [json.loads(r["raw"]) for r in b.ch(
        f"SELECT raw FROM {source_table(b, meta, e)} WHERE entity={sql_string(e)} ORDER BY id FORMAT JSONEachRow")]
                for e in ("domains", "fields", "subfields", "topics")}
    concepts = b.ch(f"SELECT id,JSONExtractString(raw,'display_name') name,arrayMap(a -> JSONExtractString(a,'id'),JSONExtractArrayRaw(raw,'ancestors')) ancestors FROM {source_table(b,meta,'concepts')} WHERE entity='concepts' ORDER BY id FORMAT JSONEachRow")
    names = sql_list(n.casefold() for n in p["journal_names"])
    journals = [json.loads(r["raw"]) for r in b.ch(f"SELECT raw FROM {source_table(b,meta,'sources')} WHERE entity='sources' AND JSONExtractString(raw,'type')='journal' AND (lowerUTF8(JSONExtractString(raw,'display_name')) IN {names} OR match(JSONExtractString(raw,'display_name'),{sql_string(earth_rules.unicode_regex(p['journal_name_pattern']))})) ORDER BY id FORMAT JSONEachRow")]
    rules = earth_rules.compile_rules(p, taxonomy, concepts, journals, (legacy or {}).get("rules", []))
    atomic_json(generation_path(g) / "earth-rules-v2.json", rules)
    return rules


def implementation_signature():
    return digest({p.name: file_hash(p) for p in (Path(__file__), Path(earth_rules.__file__))})


def upgrade_unicode(b, g, state, rules):
    """Clone immutable old facts, preserving progress; repair before any author propagation."""
    from .resources import Paused
    if state.get("implementation_signature") != ASCII_IMPLEMENTATION or state["phase"] != "evidence" or state.get("finalized_count",0):
        raise RuntimeError("classification upgrade requires a known unfinished ASCII-boundary catalog")
    source = state["facts_table"]
    active = b.ch("SELECT count() n FROM system.processes WHERE startsWith(query,"+sql_string("INSERT INTO "+source+" ")+") FORMAT JSONEachRow")[0]["n"]
    if active: raise Paused("previous classification insert is still draining")
    code = implementation_signature()
    signature = digest(dict(version=earth_rules.VERSION,rules=rules,metadata=state["metadata_signature"],implementation=code))
    prefix = database(g)+".search_"
    target = prefix+"facts_"+signature[:12]
    # Native CLONE shares immutable parts. Later replacements cannot change the original table.
    b.ch(f"CREATE TABLE IF NOT EXISTS {target} CLONE AS {source}")
    atomic_json(generation_path(g)/"search-catalog-before-unicode.json",state)
    result = dict(state,state="building",phase="unicode_repair",signature=signature,implementation_signature=code,
                  facts_table=target,table=prefix+"membership_"+signature[:12],authors_table=prefix+"authors_"+signature[:12],
                  repair_source_table=source,repair_end=state["after"],repair_after="",repair_checked=0,repair_written=0,
                  repair_started=time.time(),updated=time.time(),
                  repaired_from={k:state[k] for k in ("signature","facts_table","count","after","implementation_signature")})
    result.pop("error",None)
    return result


def build_catalog(g):
    from .ingest import lock
    from .resources import checkpoint
    from .search_scopes import catalog_path
    from .search_versions import require_metadata
    from .official import source_table
    proof = require_metadata(g)
    with lock(generation_path(g) / "search-catalog.lock"):
        b = Backend(writer=True)
        try:
            path = catalog_path(g)
            prior = json.loads(path.read_text()) if path.exists() else None
            if prior and prior.get("metadata_signature") != proof["signature"]:
                raise RuntimeError("catalog source metadata changed")
            if prior and prior["rule_version"] == earth_rules.VERSION:
                state = prior
                rules = json.loads((generation_path(g) / "earth-rules-v2.json").read_text())
                if digest(rules) != state["rules_signature"]:
                    raise RuntimeError("frozen Earth rules changed")
                if state["state"] == "complete": return state
                if state["state"] == "failed":
                    raise RuntimeError("failed Earth classification requires investigation before resuming")
                if digest(earth_rules.policy()) != digest(rules["policy"]):
                    raise RuntimeError("Earth classification policy changed mid-build")
                if implementation_signature() != state["implementation_signature"]:
                    state = upgrade_unicode(b,g,state,rules)
                state.update(state="building")
                state.pop("error", None)
                atomic_json(path,state)
            else:
                if prior and prior["rule_version"] != 1:
                    raise RuntimeError("unsupported prior catalog version")
                if prior:
                    atomic_json(generation_path(g) / "search-catalog-v1.json", prior)
                rules = freeze_rules(b, g, prior)
                code = implementation_signature()
                signature = digest(dict(version=earth_rules.VERSION, rules=rules, metadata=proof["signature"], implementation=code))
                prefix = database(g) + ".search_"
                state = dict(state="building", phase="evidence", rule_version=earth_rules.VERSION,
                             metadata_signature=proof["signature"], signature=signature, rules_signature=digest(rules),
                             implementation_signature=code, table=prefix+"membership_"+signature[:12],
                             facts_table=prefix+"facts_"+signature[:12], authors_table=prefix+"authors_"+signature[:12],
                             count=0, after="", finalized_count=0, finalized_after="", started=time.time(),
                             baseline={k:prior.get(k) for k in ("table", "signature", "count", "after")} if prior else None,
                             rule_counts={k:len(rules[k]) for k in ("domains", "fields", "subfields", "topics", "concepts", "journals")})
                atomic_json(path, state)
            facts, authors, table = state["facts_table"], state["authors_table"], state["table"]
            b.ch(f"CREATE TABLE IF NOT EXISTS {facts} (id String,version UInt64,raw_digest FixedString(64),field_ids Array(UInt16),reasons Array(String),author_ids Array(String),author_seed UInt8,author_candidate UInt8) ENGINE=ReplacingMergeTree(version) ORDER BY id")
            b.ch(f"CREATE TABLE IF NOT EXISTS {authors} (id String,core_works UInt64) ENGINE=ReplacingMergeTree ORDER BY id")
            b.ch(f"CREATE TABLE IF NOT EXISTS {table} (id String,version UInt64,raw_digest FixedString(64),field_ids Array(UInt16),owner LowCardinality(String),earth_related UInt8,reasons Array(String)) ENGINE=ReplacingMergeTree(version) PARTITION BY owner ORDER BY (owner,id)")
            expected = read_generation(g)["expected"]["works"]["records"]
            source = source_table(b, read_generation(g), "works")
            if state["phase"] == "unicode_repair":
                sql = earth_rules.evidence_sql(rules)
                while True:
                    checkpoint(g)
                    # Old negatives with no candidate have no matched text at all. A stricter
                    # Unicode boundary cannot make them positive; non-text evidence is unchanged.
                    keys = b.ch(f"SELECT id FROM {state['repair_source_table']} FINAL WHERE id>{sql_string(state['repair_after'])} AND id<={sql_string(state['repair_end'])} AND (notEmpty(reasons) OR author_candidate=1) ORDER BY id LIMIT {REPAIR_BATCH_ROWS} FORMAT JSONEachRow")
                    if not keys: break
                    ids = sql_list(r["id"] for r in keys)
                    # ASCII-only text is byte-identical under both boundary definitions.
                    text = "concat(JSONExtractString(raw,'title'),' ',JSONExtractString(raw,'display_name'),' ',arrayStringConcat(arrayMap(k -> JSONExtractString(k,'display_name'),JSONExtractArrayRaw(raw,'keywords')),' '),' ',arrayStringConcat(JSONExtractKeys(raw,'abstract_inverted_index'),' '))"
                    # Decode strings/keys before checking, including JSON escaped Unicode.
                    unicode_letters = "match(replaceRegexpAll("+text+",'[\\\\x00-\\\\x7F]',''),'[\\\\p{L}\\\\p{N}]')"
                    candidates = b.ch(f"SELECT id FROM {source} WHERE entity='works' AND id IN {ids} AND {unicode_letters} FORMAT JSONEachRow")
                    if candidates:
                        selected = sql_list(r["id"] for r in candidates)
                        b.ch(f"INSERT INTO {facts} {sql} FROM {source} WHERE entity='works' AND id IN {selected} ORDER BY id"+earth_rules.EVIDENCE_SETTINGS)
                    state.update(repair_after=keys[-1]["id"],repair_checked=state["repair_checked"]+len(keys),
                                 repair_written=state["repair_written"]+len(candidates),updated=time.time())
                    atomic_json(path,state)
                count = b.ch(f"SELECT count() n FROM {facts} FINAL WHERE id<={sql_string(state['repair_end'])} FORMAT JSONEachRow")[0]["n"]
                if count != state["count"]:
                    raise RuntimeError("Unicode repair changed acknowledged work membership")
                state.update(phase="evidence",repair_completed=time.time(),updated=time.time())
                atomic_json(path,state)
            if state["phase"] == "evidence":
                sql = earth_rules.evidence_sql(rules)
                while True:
                    checkpoint(g)
                    started = time.monotonic()
                    keys = b.ch(f"SELECT id FROM {source} WHERE entity='works' AND id>{sql_string(state['after'])} ORDER BY id LIMIT {BATCH_ROWS} FORMAT JSONEachRow")
                    if not keys: break
                    end = keys[-1]["id"]
                    b.ch(f"INSERT INTO {facts} {sql} FROM {source} WHERE entity='works' AND id>{sql_string(state['after'])} AND id<={sql_string(end)} ORDER BY id"+earth_rules.EVIDENCE_SETTINGS)
                    state.update(count=state["count"]+len(keys), after=end, updated=time.time(),
                                 seconds=state.get("seconds",0)+time.monotonic()-started)
                    atomic_json(path,state)
                count = b.ch(f"SELECT count() n FROM {facts} FINAL FORMAT JSONEachRow")[0]["n"]
                if count != expected or state["count"] != expected:
                    raise RuntimeError("Earth facts do not cover the frozen works collection")
                state.update(phase="author_seeds", updated=time.time())
                atomic_json(path,state)
            if state["phase"] == "author_seeds":
                checkpoint(g)
                threshold = rules["policy"]["author_min_core_works"]
                # Only initial core-field/journal evidence seeds authors: no recursive propagation.
                b.ch(f"INSERT INTO {authors} SELECT author,uniqExact(id) n FROM {facts} FINAL ARRAY JOIN author_ids AS author WHERE author_seed=1 GROUP BY author HAVING n>={threshold} SETTINGS max_bytes_before_external_group_by=1073741824")
                state.update(phase="finalize", author_seeds=b.ch(f"SELECT count() n FROM {authors} FINAL FORMAT JSONEachRow")[0]["n"], updated=time.time())
                atomic_json(path,state)
            if state["phase"] == "finalize":
                while True:
                    checkpoint(g)
                    keys = b.ch(f"SELECT id FROM {facts} FINAL WHERE id>{sql_string(state['finalized_after'])} ORDER BY id LIMIT {BATCH_ROWS} FORMAT JSONEachRow")
                    if not keys: break
                    end = keys[-1]["id"]
                    b.ch(f"""INSERT INTO {table}
WITH arraySort(arrayDistinct(arrayConcat(reasons,if(author_candidate=1,arrayMap(a -> concat('author:',a),arrayFilter(a -> a IN (SELECT id FROM {authors} FINAL),author_ids)),[])))) AS all_reasons
SELECT id,version,raw_digest,field_ids,if(notEmpty(all_reasons),'earth_sciences',if(empty(field_ids),'unclassified',concat('field_',toString(arrayMin(field_ids))))),toUInt8(notEmpty(all_reasons)),all_reasons
FROM {facts} FINAL WHERE id>{sql_string(state['finalized_after'])} AND id<={sql_string(end)} ORDER BY id""")
                    state.update(finalized_count=state["finalized_count"]+len(keys), finalized_after=end, updated=time.time())
                    atomic_json(path,state)
                owners = b.ch(f"SELECT owner,count() count FROM {table} FINAL GROUP BY owner ORDER BY owner FORMAT JSONEachRow")
                if sum(r["count"] for r in owners) != expected or state["finalized_count"] != expected:
                    raise RuntimeError("Earth labels do not cover the frozen works collection")
                if state.get("baseline"):
                    baseline = state["baseline"]["table"]
                    missing = b.ch(f"SELECT count() n FROM (SELECT id FROM {baseline} FINAL WHERE owner='earth_sciences') l LEFT ANTI JOIN (SELECT id FROM {table} FINAL WHERE owner='earth_sciences') r USING id FORMAT JSONEachRow")[0]["n"]
                    if missing: raise RuntimeError("new Earth rules lost baseline members")
                order = lambda r: (0,0) if r["owner"]=="earth_sciences" else (2,0) if r["owner"]=="unclassified" else (1,int(r["owner"][6:]))
                evidence = b.ch(f"SELECT splitByChar(':',reason)[1] kind,count() n FROM {table} FINAL ARRAY JOIN arrayDistinct(arrayMap(r -> splitByChar(':',r)[1],reasons)) AS reason WHERE earth_related=1 GROUP BY kind ORDER BY kind FORMAT JSONEachRow")
                state.update(state="complete", phase="complete", owners=sorted(owners,key=order), evidence_counts=evidence, completed=time.time(),updated=time.time())
                atomic_json(path,state)
            return state
        except BaseException as error:
            if 'state' in locals() and state.get("state") not in ("complete", "failed"):
                from .resources import Paused
                state.update(state="paused" if isinstance(error,Paused) else "failed", error=repr(error), updated=time.time())
                atomic_json(path,state)
            raise
        finally:
            b.close()
