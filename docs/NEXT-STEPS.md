# Next development plan

[中文](NEXT-STEPS.zh-CN.md) · [Current status](STATUS.md) · [Roadmap](ROADMAP.md)

Plan reviewed on 2026-11-11 against v0.3.2. Everything below is planned work,
not functionality delivered by that release. Keep the current service and existing
qualified indexes available while building and validating additions separately.

## Priorities and sequencing

| Milestone | Deliverable | Main acceptance condition |
| --- | --- | --- |
| M0: source and identity contracts | Read-only arXiv audit; versioned source records, identifiers, field provenance and incremental release plan | Account for every input; deterministic IDs; preserve source/unknown/conflicting fields; replay changes without duplicates |
| M1: local arXiv integration | Optional DuckDB/Parquet adapter, 2010+ first submissions, exact links to OpenAlex, supplemental views and separate unmatched records | Matching precision measured on a labeled sample; ambiguous/unmatched records retained; no fabricated OpenAlex IDs or references |
| M2: complete Earth entity access | Small reference/name catalogs, then Earth-linked publishers/funders/awards; entity-to-works navigation and supported filters | Every selected ID resolves or is explicitly missing; names, filters, pagination and exports agree; do not rebuild work documents merely for name lookup |
| M3: sustainable freshness | arXiv OAI-PMH harvesting; OpenAlex local-release diff or authorized sync-filter adapter; update/delete/redirect processing | Stopping/replaying yields the same effective release; old references disappear when replaced; metadata, search and exports pin one release |
| M4: configurable search scopes | Domain/field/topic/ID selectors, multilabel scope membership and reusable search blocks; pilot AI/CS/math/engineering intersections | Deduplicated counts/paging across overlapping scopes; existing Earth segments preserved; measured pilot predicts time/disk |
| M5: content links and downloads | Ranked existing links, opt-in PDF/TEI jobs, validated files, hashes, receipts and local availability | Valid files match the selected work/version; failure/retry states explicit; no HTML login pages accepted as PDF |

M1's source adapter and M2's independent small catalogs can progress separately
after M0. M3's release/update contract must be designed in M0 before enlarging the
OpenAlex corpus substantially. Prioritize freshness over a large new domain index
if the indexed records would soon be replaced by a newer snapshot. M5 remains later;
showing source links can precede downloading files.

## 1. Metadata supplementation and identity

An arXiv metadata database can supply abstracts, authors, submission/version dates,
subject categories, licenses, publication DOI/journal references and source links.
It generally cannot supply a structured reference list. A local normalized table
may also omit original JSON, parsed authors or intermediate version history;
audit the actual schema rather than treating it as an untouched raw snapshot.

Keep the existing OpenAlex source JSON unchanged. Store additional source records
with source, source ID, fetched/updated times, payload hash and adapter version;
preserve original payloads where available. A supplemental view should expose
field provenance and conflicts. The absence of an abstract can be supplemented
for display without claiming it was part of the original OpenAlex record.

Use explicit arXiv links/IDs and unique normalized DOI matches first. An arXiv
metadata DOI may identify the published paper, while `10.48550/arXiv.…` identifies
the preprint: these are useful associations, not interchangeable source records.
Title/author/year matching produces candidates until calibrated and reviewed.
Do not merge on title alone, invent W IDs for unmatched arXiv records, or assume
all author strings identify existing OpenAlex author entities.

Version families should retain separate source records, dates, DOI, licenses and
reference lists. Prefer a verified published version in an optional grouped view,
with access to preprints. A published version's bibliography can be shown with
its source, never copied into the preprint's original references. Preserve the
existing `/works` and `/v1/works` contracts; introduce a separately documented
supplemental/unified-source API for records without an OpenAlex ID.

Start with all 2010+ arXiv subjects as previously selected. This is a source/year
scope, not an Earth label. arXiv categories must be mapped explicitly to any
OpenAlex domain/field/topic scheme; arXiv's own `domain` strings are not OpenAlex's
four domains. Measure overlap with OpenAlex before estimating added paper counts.

## 2. Freshness: discovery is different from synchronization

| Input | Recommended use | Important limitation |
| --- | --- | --- |
| arXiv local database | Baseline import | Build time is not source coverage time; validate submission/update fields |
| arXiv OAI-PMH | Main incremental metadata harvest | Datestamps describe record changes, not first submission; honor server tokens/retry guidance |
| arXiv API | Targeted fetches and repair | Honor upstream request pacing; not the preferred full-archive harvester |
| arXiv RSS/Atom | New-paper discovery/alerts | Not a complete history or repair mechanism for an interrupted mirror |
| OpenAlex newer public snapshot | Free periodic reconciliation; user downloads to a separate input path | Current format, deletions, merged IDs and release notes must be checked |
| OpenAlex API sync filters | Authorized, scoped frequent updates | Current documentation requires an eligible subscription; ordinary free query access is not equivalent |
| OpenAlex publication-date queries | Optional best-effort discovery of recent publications | Misses old-record repairs, late additions, deletions and merges |

As checked on 2026-11-11, OpenAlex documents free quarterly snapshots and paid
daily snapshots/API sync filters. It has no release announcement feed; watch the
release notes or manifest date. Do not plan around a deprecated daily changefile
endpoint. API/RSS freshness must be reported per source, not as a single claim
that the entire old snapshot is up to date.

Keep raw fetch receipts and replayable batches, source timestamps, bounded retries,
provider-specific request pacing and committed watermarks. Revisit overlapping
windows; advance a watermark only after the intended changes are committed.
Follow arXiv's OAI response datestamps/resumption-token protocol. Recover the gap
between the audited baseline coverage and the first scheduled run before relying
on ongoing discovery.

Do not write live updates into a frozen generation and rewrite its proofs.
Design a release manifest binding the immutable base, source/enrichment delta,
tombstones/redirects and changed search segments. Readers and exports need one
pinned effective release. An updated work must replace its outgoing edge set and
adjust reverse edges; an index delta must suppress older document versions and
preserve exact counts/cursors. Simply unioning old/new indexes duplicates papers
and exposes stale content. Keep the overlay bounded and compact only after
measured cost warrants it; validate a small update/replay/recovery fixture first.

For bulk release changes, compare old/new manifests and schemas, apply the current
source deletion/merge ledgers and reconcile IDs. Some upstream bulk field changes
do not advance record `updated_date`, so read release notes and periodically
reconcile a complete release. Upstream ID merges are different from scholarly
preprint/published-version association. Query-time upstream HTTP calls must remain
optional; normal local searches should not wait on internet services.

## 3. Earth entities and wider scopes

All imported entity records are already preserved. Complete Earth entity access
means name discovery, work linkage, supported filtering and export where meaningful,
not indexing every field or adding every entity type to every filter expression.

1. Supply small domains/fields/subfields, countries/languages/licenses/types/SDGs
   as lookup dictionaries. Add keyword/concept name catalogs where useful. Fixed
   dictionaries can use existing metadata lookup rather than a separate engine.
2. Add publisher/funder names and verified work/source associations. Extract award
   membership from Earth works before deciding whether a global award index is needed.
3. Reuse existing authors/institutions/sources/topics. Preserve unresolved links and
   route-specific authorship semantics; newly introduced filters need API/web/export tests.

The current catalog builder explicitly requires a validated Earth parent and
implements authors/institutions. It is not already a general all-entity builder.
Extend its proven batch/checkpoint/seal patterns without creating a plugin framework.

A configurable search profile should select domain, field, topic, journal or ID
lists, with explicit rule versions and counts. OpenAlex has four domains, but a
whole physical-sciences domain may be much larger than an AI/CS subset. Prioritize
useful intersections before a full all-discipline build. The current catalog's
exclusive storage owner is not complete multilabel scientific membership. Use
true source hierarchy labels to select scopes and deduplicate shared work IDs;
reuse sealed blocks and add only unindexed work documents.

Before a large build, use a stratified 100k–1M-record pilot to measure indexed bytes
per work, build/seal/recovery rates and API latency under background ingestion.
Estimate time and disk from those measurements, including snapshots/parents and
headroom. Do not extrapolate an old whole-Earth rate to new abstracts/domains blindly.

## 4. Links, downloading and bibliography recovery

First normalize and rank existing landing/PDF links from source records, distinguishing
publisher/preprint/repository versions and known/unknown licenses. Then implement
explicit user-selected download jobs using available arXiv/repository links or an
optional OpenAlex content API adapter. Current OpenAlex documentation describes
metered PDF/TEI downloads; local keyless operation does not remove upstream key,
usage or network requirements. Check content availability rather than assuming
all metadata records have files.

Reuse job receipts/checkpoints, hashes and downloads concepts from exports, while
keeping content files independent of metadata exports. Verify actual file type,
nonempty/parseable content, final URL and work/version association; preserve HTTP
failure reasons. A downloader following source URLs needs redirect/private-address
validation and must not forward provider credentials to unrelated hosts. These
checks become relevant when the download feature is implemented.

For missing bibliographies, prefer explicit published-version references or
available TEI bibliography extraction before introducing PDF parsing. Extracted
citations need matching confidence, provenance and reviewable unresolved entries.
Do not silently convert an uncertain match into a confirmed OpenAlex reference.
Content caching/downloads do not require reindexing existing literature titles.

## Modules and validation

Extend the existing processing chain with concrete modules: optional arXiv source
adapter, identity/version links, provenance/supplemental projection, sync receipts
and release manifest, generalized scope/entity selection, and later content jobs.
Each needs independently callable CLI steps, API/UI visibility, pinned inputs and
outputs, durable checkpoints, cancellation/replay and explicit acceptance evidence.
This is a scoped extension of ClickHouse + Manticore, not a replacement stack.

Add source coverage/freshness and missingness dashboards, labeled identity samples,
update/delete/merge replay fixtures and representative query/export benchmarks.
Keep independent data backup/recovery work alongside these additions. If a feature
changes a work's indexed fields, rebuild only affected projections in a new build;
name-only catalogs, source links and content jobs need not rebuild all Earth works.

## Official references

Verified for planning on 2026-11-11; service policies can change.

- [OpenAlex synchronization](https://help.openalex.org/access/sync/)
- [OpenAlex common attributes](https://help.openalex.org/data/common-attributes/)
- [OpenAlex full text](https://help.openalex.org/access/fulltext/)
- [arXiv OAI-PMH](https://info.arxiv.org/help/oa/index.html)
- [arXiv API manual](https://info.arxiv.org/help/api/user-manual.html)
- [arXiv RSS/Atom](https://info.arxiv.org/help/rss.html)
