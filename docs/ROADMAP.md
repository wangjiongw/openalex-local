# Delivery status and follow-up work

## Candidate v0.3.0-rc.1

The local snapshot reader, full raw metadata and citation import/audit, metadata API/web service, optional Earth collections and Manticore indexes, native installers and search recovery are implemented. This candidate adds Earth-linked author and full institution catalogs, query combinations, durable works exports and corresponding web controls. Existing work indexes are reused; identity-to-work candidates accelerate mandatory identity filters without changing authorship semantics.

See [the feature inventory](FEATURES.zh-CN.md), [query/export operations](SEARCH-EXPORT.md), [validation](VALIDATION.md) and [release boundaries](RELEASE.md). Snapshot acquisition stays user-managed. Runtime datasets, engine binaries, private configuration and exports stay outside Git.

## Candidate feedback and stable release

Fix reproduced defects within the candidate scope, maintain compatibility and validation evidence, then consider promoting to v0.3.0. There is no fixed 24-hour wait or automatic data/service cutover merely from publishing a Git tag. Workspaces require explicit build, validation and publication.

## Optional later work

- PDF/TEI download integration after the query/export workflow.
- More entity-name indexes, user-defined text scopes and broader filter optimization.
- Calibrated bibliographic matching and cross-record version associations.
- Parquet input, new-snapshot updates, deleted/merged ID application.
- Independent ClickHouse disaster recovery, semantic retrieval, SDK and MCP.

These are not prerequisites for using this candidate. Official-style routes do not imply complete live OpenAlex API parity. External A/D graph writing remains a separate project.
