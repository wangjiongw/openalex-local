# Conservative version relationships

Read `AGENTS.md`, `DEVELOPMENT.md` and the reader guide `../VERSIONS.md`.

- Schema 1: `version_groups.py` owns decisions and conflict-free publication stars.
  `versions_pipeline.py` owns independent durable releases and ClickHouse tables.
  `versions_api.py` hydrates a published group's original members.
- Keep original work JSON, reference edges, source signatures and search projections
  unchanged. Reuse `arxiv_pipeline.exact_links/LinkEvidence` and existing candidates;
  never promote title, shared DOI, citation or related-work similarity by itself.
- Freeze source staging bytes, arXiv release identity, metadata proof, collection,
  search manifest, sample seed/quotas and processing signature. Do not patch a live
  release's identity to accept a code/input change; create another release.
- New evidence is limited to saved pilot IDs. All existing selected arXiv seeds
  are replayed; all confirmed targets are verified. These are separate coverage counts.
- SQLite uses one owner, FULL synchronization and DELETE journals. Cursor and batch
  writes commit together. Replays use ReplacingMergeTree/FINAL and full reconciliation.
- A one-target source is not a version group. Competing publications and notices
  block stars; no transitive multi-publication group. Preserve review evidence.
- Optional module absence must preserve ordinary API readiness. `/versions` remains
  arXiv-source semantics; new routes use `/version-group` and `/version-groups`.
- Preserve matched/displayed distinction and manual choice in the UI. Raw downloads,
  selected/query exports, paging and counts retain original-record semantics.
- Validate in an isolated workspace and native fixture; browser tests cover version
  preference, switching, references and group JSON download. Real-data qualification,
  publication, deployment and community release remain distinct evidence gates.
- Retain prior pointers, relationship/source staging, dependencies and runtimes.
  Roll back by validating/publishing a retained compatible release, not editing hashes.

## Schema 2 full-index candidate profile

- Read `../VERSION-MATCHING.md`. Keep `version_groups.py` and `versions_pipeline.py`
  byte-identical: published schema-1 releases hash those files.
- `versions_reader.py` dispatches both formats. `versions_full.py` owns independent
  features, candidate batches, review packages, cumulative groups and publication;
  `version_matching.py` owns pure evidence and acceptance rules.
- Default full-index run stops at review. Never synthesize audit labels. Blind
  sets and thresholds are fixed before labels; a consumed holdout cannot be reused
  for tuning. Unaudited strata remain review. Approximate matching never rewrites
  raw records or changes ordinary search/export semantics.
- Feature directories bind the frozen index and posting projection. Reuse them for
  compatible confirmation changes, with a new relationship release/signature.
  Pilot sampling excludes prior processed IDs; stratum batches are cumulative.
- Only schema-compatible software may publish schema-2 active pointers. Upgrade
  software against the old pointer first. Preserve old runtimes and pointers;
  candidate cursors pin their relationship release.
