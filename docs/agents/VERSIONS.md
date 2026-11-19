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
- Full-index defaults to automatic rules and runs through validate; publication
  remains explicit. `version_auto_review.py` owns the frozen conservative policy.
  Record automatic provenance, no independent attestation or precision bound.
  Verify original digests/notices and retain unresolved candidates. Automatic
  mode has no human sample quotas. Changed assessment needs a new release and
  separately verified reuse of compatible completed features/candidates.
- Optional independent mode stops at review. Never synthesize audit labels. Blind
  sets and thresholds are fixed before labels; a consumed holdout cannot be reused
  for tuning. Uncalibrated strata in independent mode remain review. Matching never rewrites
  raw records or changes ordinary search/export semantics.
- Feature directories bind the frozen index and posting projection. Reuse them for
  compatible confirmation changes, with a new relationship release/signature.
  Pilot sampling excludes prior processed IDs; stratum batches are cumulative.
- Only schema-compatible software may publish schema-2 active pointers. Upgrade
  software against the old pointer first. Preserve old runtimes and pointers;
  candidate cursors pin their relationship release.

- `version_review.py` is included in schema-2 implementation signatures. Review
  stages compact paged records in durable SQLite, commits cursor/data together,
  retains first-direction deduplication and all graph edges, and partitions each
  component once. Do not replace independent study partitions with row sampling.
  Existing packages must match committed staging; interrupted public exports can
  be rebuilt without resampling. Changed implementations require a new release;
  bind reused completed tables to semantic identities and content/count proofs.
- Feature rule v2 normalizes SQL JSON scalar retraction flags before notice
  detection and rejects unknown values. v1 could turn string zero into a truthy
  blocked flag. Rebuild affected feature payloads, candidate decisions and review
  packages under a new release; unchanged metadata/text indexes remain usable.
  Never clear all blocked flags or relax review gates to repair this defect.

## rc9 execution and read compatibility

- Keep `version_matching.py` and automatic acceptance rules unchanged. Source-level
  scoring lives in `version_scoring.py`; spawn workers own no HTTP clients/writers.
  Preserve deterministic task results and a single commit owner, with one-batch
  lookahead and a configurable process-tree RSS guard (64 GiB default).
- `version_group_batch.py` owns bounded star commits. Verify groups/members/candidate
  payloads before the anchor cursor. Never repeat the parent import over already
  extended groups; bind its receipt to parent and producer identities.
- `version_resume.py` permits exact registered historical producers only. Running
  and publication still require the current signature. Migrate paused source data
  to a new release with matching semantics; verify ordered content hashes, coverage
  and cursor bounds. Do not copy uncommitted tails or partial assessment results.
- `version_progress.py` distinguishes fully checked source coverage, associated
  sources inside that coverage and candidate-generation visits. Global inherited
  members and groups are different scopes/units. Keep HTTP progress reads cheap;
  optional progress errors must not disable otherwise valid published groups.

## rc10 bounded assessment

- `version_assessment.py` owns automatic assessment SQLite staging, durable page
  cursors, first-direction star deduplication, global competing-anchor queries and
  feature-hash verification receipts. Never split conflict decisions by chunk.
- Commit unresolved ClickHouse reviews before SQLite cursor/data; replay is
  idempotent. Resume assessment and raw verification without retaining the full
  candidate set in Python. Retain staging with its release and input identity.
- `version_memory_gib` is an installation setting, default 64, not a policy knob.
  Keep the RSS guard enabled. Raising it requires measured headroom; do not use
  it as a substitute for bounded processing.
- Migration v2 receipts hash engine-side SHA256 payload digests in ID order.
  Preserve historical producer registrations and old manifests; new producers
  require a new release even when the decision policy is unchanged.
