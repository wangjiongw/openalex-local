# Plan for version groups within OpenAlex

[中文](VERSION-GROUPS-PLAN.zh-CN.md) · [Development plan](NEXT-STEPS.md)

Status: the first detail-only stage is implemented in the v0.5.0 candidate; data validation/publication and deployment are separate gates. See the [operating workflow](VERSIONS.md). Existing arXiv exact DOI evidence and qualified ClickHouse/Manticore indexes are reused. Grouped search remains a later design, not an available feature.

## Goal and coverage

Use `/status.search_coverage.earth_complete` and the actual build scope to assess Earth search readiness. Completing a frozen collection does not establish exhaustive scientific recall or text coverage of every interdisciplinary paper. Report imported metadata, rule membership, indexed scope and missing fields separately. Source coverage dates are not a live-update guarantee.

Identify independent records representing versions of one study and prefer confirmed publication display without deleting or rewriting OpenAlex W IDs. Official merged-ID redirects, duplicate records, preprint/publication versions, corrections, retractions and extended studies are different relationship types.

## Evidence and confirmation

| Evidence | Treatment |
| --- | --- |
| Confirmed preprint/publication DOI associations in the published arXiv source | Reuse as initial relationship seeds between OpenAlex records |
| Official merged-ID/redirect inputs | Process only when actual versioned inputs are available and verified; identifier resolution is distinct from study-version identity |
| Different OpenAlex IDs sharing a DOI | Duplicate/conflict candidates requiring title, author, type and other identifier checks; do not bypass existing DOI ambiguity protection |
| Explicit arXiv identifiers/source links or typed version relations | Preserve evidence, verify target identity and relation meaning, then confirm |
| Similar titles, authors and dates | Candidates only until calibrated against labeled evidence |
| Corrections, retraction notices and extended conference-to-journal studies | Separate typed relations by default; do not collapse without explicit evidence |

`related_works`, shared authors or citation edges alone do not prove version identity. Generic/reused titles, incorrect DOIs, different-author homonyms and missing authors are necessary negative cases. Similar A–B and B–C links must not automatically confirm an A–B–C group.

## Modular workflow

1. **Freeze inputs and extract evidence.** Pin metadata generation, arXiv source release, selected Earth scope and rules. Reuse arXiv links before scanning bounded Earth seeds. Resolve targets against all imported metadata. Preserve original identifiers/links and payload hashes.
2. **Retrieve candidates and resolve identity.** Reuse `Backend.resolve_many/details`, normalization and existing title/author candidate retrieval. Sort keys, batch reads, bound concurrency and save checkpoints rather than repeatedly scanning the full corpus.
3. **Judge relations.** Reuse the exact-evidence principles of `arxiv_pipeline.exact_links`. Store separate internal relation types, evidence, rule versions and confirmed/candidate/conflict/rejected decisions with reasons; do not rewrite the existing source rule signature.
4. **Store and publish independently.** Separate ClickHouse tables hold edges, group membership, display representatives and review history. Pin proofs/scope/statistics in a release manifest and retain rollback inputs. Local group IDs never impersonate W IDs; retain cross-release mappings and split/merge history.
5. **Serve a shared view.** API/web/exports use one published relationship release and expose the matched record, group, evidence and preferred display record. Original `/works` JSON and reference tables remain unchanged.

This does not depend on a full-discipline text index. Exact ID/DOI lookup uses all imported metadata; title candidate recall remains limited by published text coverage. A missing publication record stays unresolved rather than being invented.

## Two delivery stages

**First: version details and explicit selection.** Introduce separate group-reading contracts and optional detail views showing matched/publication/other versions and evidence. Existing `/v1/works/{id}/versions` currently returns arXiv source records; retain that response meaning. Declare new routes/fields at implementation rather than presenting them as available now. This stage normally needs no work text reindex.

**Second: optional grouped search.** Keep original-record retrieval as the default. Define a group hit as a matching member, with text and year/OA predicates satisfied on the same member. Display representatives may differ from matched members and must be labeled. Rank by matching evidence with stable tie-breaking.

Compute group counts, pagination and exports from the complete matched set, reporting record and group counts separately. Folding just one page is insufficient. Pin cursors to query, index build and relationship release. Export either all source records or one representative per group with membership/evidence. If grouping needs a new attribute or auxiliary index, benchmark it and rebuild only affected components; do not promise that complete grouped search can never require additional indexing.

## Fields and citation semantics

- Prefer a confirmed publication but keep member switching and visible correction/retraction/version differences.
- Preserve abstracts, authors, OA, licenses, dates, DOI and PDF links per source/version. Do not transfer one version's OA or license status to another.
- References belong to the selected record. Offer separately labeled associated-publication references; optional unions retain original entries, target IDs and unresolved items.
- Deduplicate actual citing records instead of adding member `cited_by_count` values. Distinguish original counts from derived group counts.
- Richer metadata does not recover a missing bibliography. Crossref starts with a small coverage trial; full-text parsing remains later and ordinary queries stay offline.

## Pilot and acceptance

Pilot 10k–100k stratified Earth candidates covering confirmed arXiv seeds, missing DOI/changed titles, distinct same-title papers, repeated DOIs, corrections and retractions. Use human-reviewed positive/negative labels; report precision, confidence intervals, candidate recall assessment and review workload. Self-generated candidates are not complete ground truth.

Verify traceable evidence, conflict rejection, single/multiple members, non-transitive conflicts, checkpoint replay, controlled splits, rollback and compatibility of original DOI/ID lookup. API/web/export results must agree. Grouped search additionally requires exact complete-set counts, no cross-page duplicates/missing groups, correct member-level filtering and representative load measurements.

Measure candidate/detail throughput, relationship storage and query latency before estimating expansion. Do not begin unbounded fuzzy all-pairs comparisons or rebuild the existing Earth index for the pilot. Historical checksum failures must stop safely with evidence rather than being retried indefinitely as routine network errors.

## Position in the roadmap

M2 publisher/funder/award catalogs remain independently useful. Deliver confirmed relations/details before grouped search. Establish M3 source updates/deletions/redirects and relationship delta publication before large M4 work-index expansion. Choose cross-disciplinary scopes from actual demand and pilot costs; M5 downloading and external bibliography recovery remain separate.
