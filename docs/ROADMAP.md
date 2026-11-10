# Community delivery plan

1. Publish a clean MIT-licensed baseline, portable examples and basic CI.
2. Separate installed code and workspace; read user-managed local snapshots with
   old/new JSONL adapters, resumable metadata processing and pinned native engines.
   Snapshot downloading is documented, not implemented by the project.
   Earth-science classification and text indexing are an optional example.
3. Synthetic native pipelines pass on Linux x86_64 and ARM64. Read-only
   existing-data query checks are separate from full-data recovery qualification;
   the package remains a development release. See [validation status](VALIDATION.md).

Official-style common work queries take priority. Full-text scope, entity-name
catalog coverage, metadata scan predicates and unsupported operations must be
reported separately. Preprint/version grouping, Parquet import, live incremental
sync and full-discipline indexing are later changes. No A/D graph writing is in
scope. Existing private datasets and services are not modified by this repository.
