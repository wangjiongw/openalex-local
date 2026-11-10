# Community delivery plan

1. Publish a clean MIT-licensed baseline, portable examples and basic CI.
2. Separate installed code and workspace; add frozen-manifest JSONL download,
   old/new input adapters, resumable stages and platform-pinned engines.
3. Verify a synthetic snapshot end to end on Linux x86_64 and ARM64; validate
   existing-data adoption independently before claiming v0.2.0 readiness.

Official-style common work queries take priority. Full-text scope, entity-name
catalog coverage, metadata scan predicates and unsupported operations must be
reported separately. Preprint/version grouping, Parquet import, live incremental
sync and full-discipline indexing are later changes. No A/D graph writing is in
scope. Existing private datasets and services are not modified by this repository.
