# Delivery status and follow-up work

## Frozen for v0.2.0-rc.1

The local JSONL snapshot reader, metadata and citation import/audit, default
metadata service, optional Earth collections and Manticore index workflow,
CLI/API/web UI, native installers and search restore tools are implemented.
Validated import, indexing, publication and recovery are reusable capabilities;
they are not pending reimplementations. See the [feature inventory](FEATURES.zh-CN.md).

The candidate keeps existing interfaces and data layouts. Scope and known limits
are in [release and operations](RELEASE.md). Candidate acceptance checks source,
installed artifacts and isolated existing-data access; it does not replace the
internal production service or rebuild its published indexes.

## After candidate feedback

Fix reproduced defects within the frozen scope, maintain compatibility and
validation evidence, then decide whether to promote to `v0.2.0`. There is no fixed
24-hour wait and no automatic production cutover.

## Optional later work

- General query-result/DOI-list export and PDF/TEI download integration.
- More entity-name indexes, user-defined indexing scopes and query combinations.
- Calibrated bibliographic matching and cross-record version associations.
- Parquet input, new-snapshot updates, deleted/merged ID application.
- Independent ClickHouse disaster backup/recovery, semantic retrieval and MCP.

These items are not prerequisites for using the candidate. Official-style routes
do not imply complete live OpenAlex API parity. External A/D graph writing is a
separate project. Snapshot acquisition stays user-managed.
