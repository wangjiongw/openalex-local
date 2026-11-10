from .config import RESOURCE_ROOT
"""Versioned, resumable bibliographic collections over immutable metadata."""
import csv
import copy
import gzip
import hashlib
import heapq
import itertools
import json
import os
import re
import time
import unicodedata
from pathlib import Path

from .backend import Backend, sql_string, sql_list
from .config import ROOT, atomic_json, generation_name, generation_path, read_generation, config
from .earth_rules import tail
from .ingest import lock
from .model import abstract, doi
from .proofs import digest, file_hash

POLICY = RESOURCE_ROOT / "earth-journal-scopes.json"
GROUPS = ("01_core_journals", "02_broad_interdisciplinary", "03_author_supported")
PAGE_ROWS = 100_000
SHARD_ROWS = 100_000
SHARD_BYTES = 256 * 1024**2
CSV_FIELDS = ("openalex_id", "doi", "doi_raw", "title", "authors", "abstract", "abstract_status",
              "publication_year", "publication_date", "type", "language", "primary_source_id", "primary_source_name", "journal_names", "journal_ids",
              "is_oa", "oa_status", "oa_url", "landing_page_url", "pdf_url", "license", "publication_version",
              "cited_by_count", "is_retracted", "has_doi", "has_abstract", "group", "reasons")


def name_key(value):
    return " ".join(re.sub(r"[^\w]+", " ", unicodedata.normalize("NFKC", value).casefold().replace("&", " and ")).split())


def collection_path(g, collection):
    return ROOT / "exports/earth_collections" / generation_name(g) / generation_name(collection)


def journal_matches(record, registry):
    locations = [("primary", record.get("primary_location") or {})]
    locations += [("published", x) for x in record.get("locations") or []
                  if x.get("is_published") or x.get("version") == "publishedVersion"]
    matches = {}
    for role, location in locations:
        source = location.get("source") or {}
        sid = tail(source.get("id"))
        known = registry.get(sid)
        if known or source.get("type") == "journal":
            known = known or dict(name=source.get("display_name"), issn_l=source.get("issn_l"), scope="unknown")
            matches.setdefault(sid, dict(id=sid, name=known["name"], issn_l=known.get("issn_l"),
                                         scope=known["scope"], role=role, evidence=known.get("evidence")))
    return sorted(matches.values(), key=lambda x: x["id"])


def project(row, registry, collection, phase="direct"):
    record = json.loads(row["raw"])
    matches = journal_matches(record, registry)
    reasons = sorted(set(row.get("reasons") or []))
    direct = [r for r in reasons if not r.startswith("author:")]
    group = GROUPS[0] if any(x["scope"] == "core" for x in matches) else GROUPS[1] if direct else GROUPS[2] if reasons else None
    if group is None or (phase == "author" and group != GROUPS[2]):
        return None
    if phase == "direct" and group == GROUPS[2]:
        return None
    errors = []
    try:
        text = abstract(record.get("abstract_inverted_index")) or None
        abstract_status = "present" if text else "missing"
    except ValueError as error:
        text, abstract_status = None, "invalid"
        errors.append(dict(field="abstract", error=str(error)))
    authors = []
    for item in record.get("authorships") or []:
        author = item.get("author") or {}
        authors.append(dict(id=author.get("id"), name=author.get("display_name"), orcid=author.get("orcid"),
                            raw_author_name=item.get("raw_author_name"), position=item.get("author_position"),
                            is_corresponding=item.get("is_corresponding"), institutions=item.get("institutions") or []))
    raw_doi = record.get("doi")
    normalized = doi(raw_doi)
    if raw_doi and not normalized:
        errors.append(dict(field="doi", error="invalid DOI retained in doi_raw"))
    oa = record.get("open_access") or {}
    best = record.get("best_oa_location") or {}
    core_reasons = ["core_journal:" + x["id"] for x in matches if x["scope"] == "core"]
    result = dict(openalex_id=record["id"], doi=normalized, doi_raw=raw_doi,
                  doi_url="https://doi.org/" + normalized if normalized else None,
                  title=record.get("title") or record.get("display_name"), authors=authors, abstract=text,
                  abstract_status=abstract_status, primary_source=(record.get("primary_location") or {}).get("source"),
                  primary_location=record.get("primary_location"),
                  journal_matches=matches, open_access=oa, best_oa_location=best or None,
                  publication_year=record.get("publication_year"), publication_date=record.get("publication_date"),
                  type=record.get("type"), language=record.get("language"), cited_by_count=record.get("cited_by_count"),
                  is_retracted=record.get("is_retracted"), has_doi=bool(normalized), has_abstract=bool(text),
                  group=group, reasons=sorted(set(reasons + core_reasons)), collection=collection,
                  source_version=row["version"], raw_digest=row["raw_digest"], field_errors=errors)
    return result


def csv_row(record):
    oa, best = record["open_access"], record["best_oa_location"] or {}
    primary = record.get("primary_source") or {}
    row = {k: record.get(k) for k in CSV_FIELDS}
    row.update(authors="; ".join(x["name"] or x["raw_author_name"] or "" for x in record["authors"]),
               primary_source_id=primary.get("id"), primary_source_name=primary.get("display_name"),
               journal_names="; ".join(x["name"] or "" for x in record["journal_matches"]),
               journal_ids="; ".join(x["id"] for x in record["journal_matches"]),
               is_oa=oa.get("is_oa"), oa_status=oa.get("oa_status"), oa_url=oa.get("oa_url"),
               landing_page_url=best.get("landing_page_url") or (record.get("primary_location") or {}).get("landing_page_url"), pdf_url=best.get("pdf_url"),
               license=best.get("license"), publication_version=best.get("version"),
               reasons=json.dumps(record["reasons"], ensure_ascii=False))
    return row


def freeze_registry(b, g, policy):
    from .official import source_table
    source = source_table(b, read_generation(g), "sources")
    frozen = json.loads((generation_path(g) / "earth-rules-v2.json").read_text())
    names = [alias.casefold() for entry in policy["journals"] for alias in [entry["name"]] + entry.get("aliases", [])]
    # A bounded journal catalog read; no full works scan and no inferred core promotions.
    sql = (f"SELECT raw FROM {source} WHERE entity='sources' AND JSONExtractString(raw,'type')='journal' "
           "AND (match(JSONExtractString(raw,'display_name'),'(?i)earth|geo|ocean|climat|atmospher|hydrolog|cryosph|soil|sediment|tecton|mineral|palaeo|paleo|seism|remote sensing|environment|ecolog|forestr|water|planet|rock|quaternary') "
           "OR lowerUTF8(JSONExtractString(raw,'display_name')) IN " + sql_list(names or ["__none__"]) + " "
           "OR id IN " + sql_list([j["id"] for j in frozen["journals"]] or ["__none__"]) + ") FORMAT JSONEachRow")
    candidates = {tail(j["id"]): j for row in b.ch(sql) for j in [json.loads(row["raw"]) ]}
    entries = {name_key(alias): entry for entry in policy["journals"] for alias in [entry["name"]] + entry.get("aliases", [])}
    by_name = {}
    for j in candidates.values():
        by_name.setdefault(name_key(j["display_name"]), []).append(j)
    registry, unresolved = {}, []
    for sid, j in candidates.items():
        key = name_key(j["display_name"])
        entry = entries.get(key)
        scope, evidence = "pending", None
        if entry:
            required = set(entry.get("issn", []))
            identities = {j.get("issn_l"), *(j.get("issn") or [])} - {None}
            competing = {x.get("issn_l") for x in by_name[key] if x.get("works_count", 0) > 0}
            if (required and required & identities) or (not required and len(competing) <= 1 and j.get("works_count", 0) > 0):
                scope = entry["scope"]
                evidence = dict(url=entry["url"], scope_summary=entry["scope_summary"], basis="reviewed_journal_definition")
        registry[sid] = dict(id=sid, name=j["display_name"], issn_l=j.get("issn_l"), issn=j.get("issn") or [],
                             scope=scope, evidence=evidence, homepage_url=j.get("homepage_url"),
                             works_count=j.get("works_count"), topic_profile=j.get("topics") or [])
    for entry in policy["journals"]:
        if not any(r["scope"] == entry["scope"] and name_key(r["name"]) in
                   {name_key(x) for x in [entry["name"]] + entry.get("aliases", [])} for r in registry.values()):
            unresolved.append(entry["name"])
    return dict(version=1, policy=policy, journals=registry, unresolved_policy_names=unresolved,
                counts={scope: sum(j["scope"] == scope for j in registry.values()) for scope in ("core", "cross", "pending")})


def load_catalog(g):
    from .earth_catalog import implementation_signature
    value = json.loads((generation_path(g) / "search-catalog.json").read_text())
    rules = json.loads((generation_path(g) / "earth-rules-v2.json").read_text())
    if value.get("rule_version") != 2 or value.get("phase") == "unicode_repair" or value.get("state") == "failed":
        raise RuntimeError("export requires a healthy repaired v2 catalog")
    if implementation_signature() != value["implementation_signature"] or digest(rules) != value["rules_signature"]:
        raise RuntimeError("classification inputs changed")
    return value


def initialize(b, g, collection):
    from .recovery import require_evidence
    path = collection_path(g, collection)
    metadata = require_evidence(g, "metadata-complete")
    catalog = load_catalog(g)
    if catalog["metadata_signature"] != metadata["signature"]:
        raise RuntimeError("catalog refers to different metadata")
    policy = json.loads(POLICY.read_text())
    if policy.get("version") != 1 or any(e["scope"] not in ("core", "cross") or not e["url"].startswith("https://") for e in policy["journals"]):
        raise ValueError("invalid journal scope policy")
    aliases = {}
    for entry in policy["journals"]:
        if not entry.get("name") or not entry.get("scope_summary"):
            raise ValueError("journal scope needs a name and explanation")
        for alias in [entry["name"]] + entry.get("aliases", []):
            key = name_key(alias)
            if key in aliases and aliases[key] != entry:
                raise ValueError("conflicting journal scope aliases: " + alias)
            aliases[key] = entry
    identity = dict(metadata=metadata["signature"], catalog=catalog["signature"], policy=digest(policy),
                    exporter=file_hash(Path(__file__)), schema=1)
    manifest = path / "manifest.json"
    if manifest.exists():
        state = json.loads(manifest.read_text())
        if (state["identity"] != identity or file_hash(path / "journals.json") != state["registry_file_hash"]
                or file_hash(path / "journal-scopes.csv") != state["registry_csv_hash"]):
            raise RuntimeError("collection inputs changed; choose a new collection version")
        return state
    path.mkdir(parents=True, exist_ok=True)
    registry = freeze_registry(b, g, policy)
    atomic_json(path / "journals.json", registry)
    state = dict(generation=g, collection=collection, state="partial", phase="direct", identity=identity,
                 signature=digest(identity), registry_file_hash=file_hash(path / "journals.json"),
                 after="", scanned=0, author_after="", author_scanned=0, blocks=0,
                 counts={x: 0 for x in GROUPS}, missing={x: dict(doi=0, abstract=0, oa=0, invalid_abstract=0) for x in GROUPS},
                 started=time.time(), updated=time.time(), catalog_complete=False,
                 registry_counts=registry["counts"], unresolved_policy_names=registry["unresolved_policy_names"])
    with (path / "journal-scopes.csv.part").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(("source_id", "name", "issn_l", "scope", "evidence_url", "scope_summary"))
        for j in registry["journals"].values():
            e = j.get("evidence") or {}
            w.writerow((j["id"], j["name"], j["issn_l"], j["scope"], e.get("url"), e.get("scope_summary")))
        f.flush(); os.fsync(f.fileno())
    os.replace(path / "journal-scopes.csv.part", path / "journal-scopes.csv")
    state["registry_csv_hash"] = file_hash(path / "journal-scopes.csv")
    atomic_json(manifest, state)
    return state


def validate_receipt(path, receipt):
    for item in receipt["files"]:
        f = path / item["path"]
        if not f.is_file() or f.stat().st_size != item["bytes"] or file_hash(f) != item["sha256"]:
            raise RuntimeError("committed export file changed: " + str(f))


def apply_receipt(state, receipt):
    if receipt["sequence"] != state["blocks"] or receipt["previous_after"] != state["author_after" if receipt["phase"] == "author" else "after"]:
        raise RuntimeError("export receipt chain is inconsistent")
    state = copy.deepcopy(state)
    cursor, counter = ("author_after", "author_scanned") if receipt["phase"] == "author" else ("after", "scanned")
    state[cursor] = receipt["after"]
    state[counter] += receipt["source_count"]
    for group in GROUPS:
        state["counts"][group] += receipt["counts"][group]
        for key, n in receipt["missing"][group].items():
            state["missing"][group][key] += n
    state.update(blocks=state["blocks"] + 1, updated=time.time())
    return state


def write_block(path, state, after, source_count, records):
    seq = state["blocks"]
    if (path / "blocks" / f"{seq:08d}.json").exists():
        raise RuntimeError("refusing to overwrite a committed block")
    # Only this uncommitted sequence can leave orphan files after a crash.
    for group in GROUPS:
        for orphan in (path / group).glob(f"{seq:08d}-*"):
            if orphan.name.endswith((".csv", ".csv.part", ".jsonl.gz", ".jsonl.gz.part")):
                orphan.unlink()
    receipt = dict(sequence=seq, phase=state["phase"], previous_after=state["author_after" if state["phase"] == "author" else "after"],
                   after=after, source_count=source_count, counts={g: 0 for g in GROUPS},
                   missing={g: dict(doi=0, abstract=0, oa=0, invalid_abstract=0) for g in GROUPS}, files=[])
    handles = {}

    def close_group(group):
        item = handles.pop(group, None)
        if item is None:
            return
        gz, csv_file, writer, stem, count, size = item
        gz.close(); csv_file.flush(); os.fsync(csv_file.fileno()); csv_file.close()
        for suffix in (".jsonl.gz", ".csv"):
            temporary = path / (stem + suffix + ".part")
            with temporary.open("rb") as f:
                os.fsync(f.fileno())
            final = path / (stem + suffix)
            os.replace(temporary, final)
            receipt["files"].append(dict(path=str(final.relative_to(path)), group=group, count=count,
                                         bytes=final.stat().st_size, sha256=file_hash(final)))
        directory = os.open(path / group, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    try:
        parts = {g: 0 for g in GROUPS}
        for record in records:
            group = record["group"]
            line = (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
            if group in handles and (handles[group][4] >= SHARD_ROWS or handles[group][5] + len(line) > SHARD_BYTES):
                close_group(group)
            if group not in handles:
                stem = f"{group}/{seq:08d}-{parts[group]:03d}"
                parts[group] += 1
                (path / group).mkdir(exist_ok=True)
                gz = gzip.open(path / (stem + ".jsonl.gz.part"), "wb", compresslevel=1)
                csv_file = (path / (stem + ".csv.part")).open("w", encoding="utf-8", newline="")
                writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS); writer.writeheader()
                handles[group] = [gz, csv_file, writer, stem, 0, 0]
            item = handles[group]
            item[0].write(line); item[2].writerow(csv_row(record)); item[4] += 1; item[5] += len(line)
            receipt["counts"][group] += 1
            missing = receipt["missing"][group]
            missing["doi"] += not record["has_doi"]
            missing["abstract"] += not record["has_abstract"]
            missing["oa"] += record["open_access"].get("is_oa") is None
            missing["invalid_abstract"] += record["abstract_status"] == "invalid"
        for group in list(handles):
            close_group(group)
    finally:
        for gz, csv_file, *_ in handles.values():
            gz.close(); csv_file.close()
    atomic_json(path / "blocks" / f"{seq:08d}.json", receipt)
    return receipt


def read_records(b, source, catalog, registry, state, end, expected):
    phase = state["phase"]
    low = state["author_after" if phase == "author" else "after"]
    interval = f"id>{sql_string(low)} AND id<={sql_string(end)}"
    facts = f"(SELECT id,version,raw_digest,reasons FROM {catalog['facts_table']} FINAL WHERE {interval})"
    entities = f"(SELECT id,raw,version,digest FROM {source} WHERE entity='works' AND {interval})"
    if phase == "author":
        facts = f"(SELECT id,version,raw_digest,reasons FROM {catalog['table']} FINAL WHERE owner='earth_sciences' AND {interval} AND arrayExists(r -> startsWith(r,'author:'),reasons))"
    check = b.ch(f"SELECT count() n,countIf(e.version!=f.version OR e.digest!=f.raw_digest) bad FROM {entities} e INNER JOIN {facts} f USING id FORMAT JSONEachRow")[0]
    if check["n"] != expected or check["bad"]:
        raise RuntimeError("export metadata/facts coverage or identity mismatch")
    core = [sid for sid, j in registry.items() if j["scope"] == "core"]
    source_ids = "arrayConcat([JSONExtractString(e.raw,'primary_location','source','id')],arrayMap(l -> JSONExtractString(l,'source','id'),arrayFilter(l -> JSONExtractBool(l,'is_published') OR JSONExtractString(l,'version')='publishedVersion',JSONExtractArrayRaw(e.raw,'locations'))))"
    core_ids = "[" + ",".join(sql_string(sid) for sid in core) + "]"
    eligible = f"notEmpty(f.reasons) OR hasAny(arrayMap(s -> extract(s,'[^/]+$'),{source_ids}),{core_ids})"
    sql = f"SELECT e.id id,e.raw raw,e.version version,e.digest raw_digest,f.reasons reasons FROM {entities} e INNER JOIN {facts} f USING id WHERE {eligible} ORDER BY id FORMAT JSONEachRow"
    previous = ""
    for row in b.ch_stream(sql):
        if row["id"] <= previous:
            raise RuntimeError("export input IDs are not unique and ordered")
        previous = row["id"]
        record = project(row, registry, state["collection"], phase)
        if record:
            yield record


def status(g, collection):
    path = collection_path(g, collection)
    state = json.loads((path / "manifest.json").read_text())
    catalog = load_catalog(g)
    return dict(state, path=str(path), current_catalog={k: catalog.get(k) for k in ("state", "phase", "count", "after")})


def run(g, collection="earth_scope_v1", follow=False, max_blocks=None):
    from .official import source_table
    from .manage import resource_status
    from .resources import decide, Paused
    path = collection_path(g, collection)
    with lock(path / "export.lock"):
        b = Backend(writer=True)
        state = None
        try:
            state = initialize(b, g, collection)
            state.pop("error", None)
            registry = json.loads((path / "journals.json").read_text())["journals"]
            source = source_table(b, read_generation(g), "works")
            first_block = state["blocks"]
            while True:
                receipt = path / "blocks" / f"{state['blocks']:08d}.json"
                if receipt.exists():
                    value = json.loads(receipt.read_text()); validate_receipt(path, value); state = apply_receipt(state, value)
                    atomic_json(path / "manifest.json", state)
                    continue
                if max_blocks is not None and state["blocks"] - first_block >= max_blocks:
                    state.update(state="partial", updated=time.time()); break
                catalog = load_catalog(g)
                if catalog["signature"] != state["identity"]["catalog"]:
                    raise RuntimeError("classification catalog changed during export")
                sample = resource_status()
                reason = decide(sample["disk_free_bytes"], sample["total_rss_bytes"], policy=config())
                if reason:
                    raise Paused("export-only: " + reason)
                if state["phase"] == "direct":
                    keys = b.ch(f"SELECT id FROM {catalog['facts_table']} FINAL WHERE id>{sql_string(state['after'])} AND id<={sql_string(catalog['after'])} ORDER BY id LIMIT {PAGE_ROWS} FORMAT JSONEachRow")
                else:
                    keys = b.ch(f"SELECT id FROM {catalog['table']} FINAL WHERE owner='earth_sciences' AND id>{sql_string(state['author_after'])} AND arrayExists(r -> startsWith(r,'author:'),reasons) ORDER BY id LIMIT {PAGE_ROWS} FORMAT JSONEachRow")
                if keys:
                    state.update(state="running", updated=time.time()); atomic_json(path / "manifest.json", state)
                    value = write_block(path, state, keys[-1]["id"], len(keys),
                                        read_records(b, source, catalog, registry, state, keys[-1]["id"], len(keys)))
                    state = apply_receipt(state, value); atomic_json(path / "manifest.json", state)
                    continue
                if state["phase"] == "direct" and catalog["state"] == "complete":
                    if state["scanned"] != read_generation(g)["expected"]["works"]["records"]:
                        raise RuntimeError("export did not scan all frozen works")
                    state.update(phase="author", catalog_complete=True, updated=time.time())
                    atomic_json(path / "manifest.json", state)
                    continue
                if state["phase"] == "author":
                    state.update(state="complete", completed=time.time(), updated=time.time()); break
                state.update(state="waiting_for_scan" if catalog["phase"] == "evidence" else "waiting_for_authors", updated=time.time())
                atomic_json(path / "manifest.json", state)
                if not follow:
                    break
                time.sleep(30)
        except BaseException as error:
            if state is not None:
                state.update(state="paused" if isinstance(error, Paused) else "failed", error=repr(error), updated=time.time())
                atomic_json(path / "manifest.json", state)
            raise
        finally:
            if state is not None:
                atomic_json(path / "manifest.json", state)
            b.close()
        return state


def verify(g, collection):
    path = collection_path(g, collection)
    with lock(path / "export.lock"):
        state = json.loads((path / "manifest.json").read_text())
        if (file_hash(path / "journals.json") != state["registry_file_hash"]
                or file_hash(path / "journal-scopes.csv") != state["registry_csv_hash"]):
            raise RuntimeError("frozen journal registry changed")
        by_group = {x: [] for x in GROUPS}
        for n in range(state["blocks"]):
            receipt = json.loads((path / "blocks" / f"{n:08d}.json").read_text()); validate_receipt(path, receipt)
            for item in receipt["files"]:
                if item["path"].endswith(".jsonl.gz"):
                    by_group[item["group"]].append(item)
        counts = {}
        def ids(group):
            previous = ""
            count = 0
            for item in by_group[group]:
                with gzip.open(path / item["path"], "rt", encoding="utf-8") as f, (path / item["path"].replace(".jsonl.gz", ".csv")).open(encoding="utf-8", newline="") as c:
                    for raw, flat in itertools.zip_longest(f, csv.DictReader(c)):
                        if raw is None or flat is None:
                            raise RuntimeError("CSV/JSONL cardinality mismatch")
                        r = json.loads(raw); key = r["openalex_id"]
                        if key <= previous or flat["openalex_id"] != key or r["group"] != group:
                            raise RuntimeError("export ordering, group or CSV identity mismatch")
                        previous = key; count += 1
                        yield key
            counts[group] = count
        previous = None
        for key in heapq.merge(*(ids(group) for group in GROUPS)):
            if key == previous:
                raise RuntimeError("duplicate work across collection groups")
            previous = key
        if counts != state["counts"]:
            raise RuntimeError("manifest/group counts mismatch")
        result = dict(passed=True, time=time.time(), blocks=state["blocks"], counts=counts,
                      coverage=state["scanned"], complete=state["state"] == "complete", signature=state["signature"])
        atomic_json(path / "verification.json", result)
        return result
