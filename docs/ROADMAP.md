# Stable delivery and maintenance

[中文](ROADMAP.zh-CN.md) · [Documentation](INDEX.md)

v0.3.2 consolidates the search UI, reference-coverage explanations and project documentation. Optional keyless internal access remains available; authentication stays enabled by default. Existing indexes are reused. See [current status](STATUS.md).

## v0.3.0: usable local retrieval

The release goal is a practical, stable API over a user-downloaded snapshot,
with a web UI, CLI and durable exports. The metadata/citation pipeline and
optional Earth search profile are delivered. Compatible existing indexes are
reused; v0.3.0 does not rebuild works or change query semantics from the qualified
v0.3.0-rc.1 application.

See [features](FEATURES.md), [search/exports](SEARCH-EXPORT.md),
[validation](VALIDATION.md) and [operations](RELEASE.md). Source releases,
data publication and deployment remain separate. No fixed 24-hour wait applies.

## Maintenance priorities

- Fix reproduced defects in supported lookup, search, relations and exports.
- Preserve compatibility, resumability, explicit capabilities and existing data.
- Maintain regression cases and measure representative local workloads; tune
  wide filters when they affect actual use rather than adding unused features.
- Preserve recovery sources and improve independent backups. Existing search
  recovery evidence does not certify ClickHouse disaster recovery.

Broader official API parity is not a release requirement. The current supported
routes are the compatibility contract, with documented coverage and limits.

## Selected next work

The next priorities are source/identity contracts, optional local arXiv enrichment,
Earth entity coverage, reliable freshness, configurable search scopes, and later
content links/downloads. See [the staged development plan](NEXT-STEPS.md) for
inputs, dependencies and acceptance conditions. These are planned additions;
v0.3.2 remains the current stable implementation.

## Optional extensions

PDF/TEI downloads, version association, calibrated matching, additional text
scopes/entity names, Parquet input, snapshot updates/deleted/merged IDs, semantic
retrieval, SDK and MCP are separate future tasks. They do not block this release
or require replacing the current engines. External A/D graph writing remains
outside the core project. Snapshot acquisition stays user-managed; private data,
engines, credentials and outputs remain outside Git.
