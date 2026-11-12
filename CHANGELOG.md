# Changelog

[中文](CHANGELOG.zh-CN.md) · [Documentation](docs/INDEX.md)

## 0.4.0 — v0.4.0

- Add optional Kaggle arXiv ingestion, provenance, resumable exact version associations and independent source storage/index qualification.
- Expose source-specific API/web search, confirmed publication-first details and pinned exports through the existing worker.
- Reuse existing OpenAlex indexes; source publication and software upgrades remain explicit and independent.

## 0.3.2 — v0.3.2

- Consolidate the responsive search workspace, filter reset/active hints and export selection counts.
- Explain snapshot reference gaps, separate relation failure states and retain unresolved target IDs.
- Show the running service version and synchronize bilingual project status/maintenance documentation.
- Reuse existing metadata/indexes; API query semantics and authenticated defaults are unchanged.

## 0.3.1 — v0.3.1

- Add explicit `auth_required: false` for keyless internal deployments; authentication remains enabled by default and invalid flag types are rejected.
- Automatically connect the web UI without a key, including export polling/downloads; report the policy in status, capabilities and OpenAPI.
- Preserve request limits and anonymous rate-limit accounting; keep existing data, keys and indexes.

## 0.3.0 — v0.3.0

- Promote the qualified retrieval/export implementation to a stable local-service release; only the application/package version changes.
- Reuse existing metadata, references and published indexes; freeze supported API behavior and keep official-parity extensions optional.

- Paired English/Chinese reader guides, shared concepts, workflow diagrams and documentation navigation.
- Agent-specific development instructions, implementation map and persistent-state/verification contracts.
- Clarified the common internal/community package and recorded post-upgrade capacity separately from isolated qualification.

## 0.3.0rc1 — v0.3.0-rc.1

- Durable, pinned works exports through API, CLI and web: JSONL.gz, CSV, DOI and ID lists; chunk hashes, cancellation, checkpoint recovery and independent worker.
- Incremental full institution and Earth-linked author catalogs reuse published work segments; parallel author membership extraction, snapshot/replay and explicit validation/publication.
- Author identity, institution and language filters, entity-to-works navigation, saved query URLs and batch selection in the web UI.
- Independent Earth bibliography regressions and query/export mixed-load qualification; native end-to-end checks include catalog publication and export downloads.
- Recoverable author/institution-to-work candidate indexes accelerate mandatory identity filters in queries and exports while retaining the original authorship predicate; large/negative/optional predicates keep exact scanning.
- Large identity candidates use the same Manticore HTTP SQL endpoint with a forced docid lookup, retaining native result counts and scroll tokens.
- Batch identifier resolution avoids per-record metadata queries. Source code and workspace remain separate for supervised services; read-only adoption writes new exports locally.


## 0.2.0rc1 — v0.2.0-rc.1

- Handle dotted snapshot directories when importing persisted Manticore tables,
  using a temporary path alias without changing snapshot bytes or index layout.

- Removed `oa snapshot plan/download`; snapshot acquisition is now documented
  as a user-managed step. `snapshot verify` remains local-only.
- Default pipeline profile is now `metadata`; saved Earth plans must resume with
  `--profile earth_sciences`. `engines ensure --search` opts into Manticore.

- Community baseline extracted with a fresh public history.
- MIT licensing, contributor documentation, source/data separation policy.
- Separate installed code, packaged rules and local data workspaces.
- User-managed snapshot download guidance and local legacy/current JSONL adapters.
- Metadata-only default processing; Earth text indexing is an explicit optional example.
- Hash-pinned Linux amd64/arm64 native engines with offline cache support.
- Resumable metadata processing; optional Earth collections, indexing and search validation.
- Source-derived query/load checks and isolated fresh-engine search recovery.
- Guarded read-only adoption of explicitly compatible published installations.
- Native synthetic end-to-end CI; full production parity is not claimed.

Candidate scope is frozen. It preserves the existing metadata and search layouts;
internal `0.1.0` deployments are not automatically upgraded. Existing published
indexes can be reused only under the documented projection/version checks.
See [release and operations](docs/RELEASE.md) for supported commands and limits.
