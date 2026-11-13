# Current project status

[中文](STATUS.zh-CN.md) · [Documentation](INDEX.md)

**Stable baseline: v0.4.1.** The project is in maintenance for its supported local
retrieval workflow. Use the service's `/status`, `/capabilities` and footer version
to identify an actual installation; a source release does not upgrade deployments
or publish a dataset. Existing compatible indexes are reused.


## Optional arXiv extension in v0.4.0

The maintained source adds M0/M1: user-downloaded Kaggle JSONL (standard input),
a read-only optional derived DuckDB adapter, shared normalization/provenance,
2010+ first-submission selection, independent resumable source releases, exact
DOI version links, separate lexical search, source switching in the web page,
publication-first confirmed details and pinned exports through the existing worker.
See the [arXiv workflow](ARXIV.md). Installation and full-source publication require
their own qualification; the v0.3.2 rollback baseline remains available.


## Delivered workflow

| Stage | Delivered behavior | Boundary |
| --- | --- | --- |
| Snapshot input | Acquisition/format instructions; configured local path, inventory and verification | Users download their own consistent snapshot; no automatic acquisition |
| Metadata | Resumable ClickHouse import, complete source JSON, identifiers and reference edges | Missing source fields remain missing; raw input is preserved |
| Optional search | Earth example, Manticore build/validation/publication, entity catalogs | Only published scopes and fields are searchable; Earth is an example, not a discipline restriction |
| API and CLI | DOI/ID lookup, lexical text, supported combined filters and relation expansion | Supported routes are the contract; full official API parity is not promised |
| Web | Compact search, common/advanced filters, reset and active-condition hints, selected-result/export counts, desktop/mobile details | Preprints remain separate records; source coverage is explicit |
| Exports | JSONL.gz/CSV/DOI/ID, pinned jobs, cancel/resume and downloads through API/CLI/web | No automatic PDF downloads or full reference-target export |
| Operations | Native Linux x86_64/ARM64 packages, workspace ownership, persistent artifacts, explicit checks and rollback guidance | Source/package, data publication and service upgrades are separate |

## Known limits that matter in daily use

- Full metadata import is distinct from text-search coverage. An unindexed paper
  can still resolve by DOI/ID or appear as a reference target.
- Empty `referenced_works` means the snapshot did not record the structured list;
  it does not prove an empty bibliography. A paper can have incoming citations
  despite lacking its own reference list. Preprint coverage can be incomplete;
  this is not a claim that all arXiv records lack outgoing references.
- With a separately published arXiv source, unique exact DOI evidence links
  versions and prefers published details. Records remain separate; absent or
  ambiguous evidence never merges them. Bibliographies belong to their own record.
- Unresolved targets, missing abstracts and DOI ambiguity remain explicit.
- Current search recovery evidence does not certify independent ClickHouse
  disaster recovery. Historical storage/checksum incidents are not declared
  root-caused by a UI/software maintenance release.
- The community default requires API keys. An internal operator can explicitly
  disable authentication for all routes, including exports and downloads.

The selected future sequence is documented in [next development steps](NEXT-STEPS.md).
The entries below remain unimplemented until their own acceptance and release.

Semantic Scholar is outside the selected scope. Crossref API/public-file enrichment
is planned as a later optional source, not implemented or required by v0.4.1.

## Maintenance and optional extensions

Maintain supported lookup/search/relations/exports, capabilities, regression cases,
paired documentation and compatible recovery. Do not replace qualified engines or
rebuild data merely to pursue official parity. Broader non-DOI version association, bibliography
recovery from external sources/full text, PDF/TEI downloads, additional text scopes,
snapshot synchronization and semantic retrieval remain separate optional tasks.
External graph-enrichment jobs are outside this project's core.

See [query coverage](API-COVERAGE.md), [validation and evidence limits](VALIDATION.md),
[operations](RELEASE.md), [roadmap](ROADMAP.md) and [feature inventory](FEATURES.md).
Source, processing tools and deployment instructions belong in Git; datasets,
indexes, credentials, runtimes, exports and machine reports stay local.

## M2 first batch (v0.4.1)

Metadata-backed small directories and hierarchy controls are implemented, reusing existing work filters and exports without reindexing. See [directories](DIRECTORIES.md). Publisher/funder/award expansion remains pending; deployment is explicit.

## Version details candidate

v0.5.0 implements an independent conservative version release, all existing arXiv seeds and a deterministic 100,000-record Earth pilot, new group API/detail selection and group JSON download. The data release and service upgrade require their own validation; inspect `/status.version_groups` for actual availability. Search counts, paging, selected/query exports and raw references stay record-based. See [the workflow](VERSIONS.md).
