# Stable delivery and maintenance

[中文](ROADMAP.zh-CN.md) · [Documentation](INDEX.md)

Current stable release: **v0.4.1**. M0/M1 delivers the optional arXiv source and exact version associations; M2 first batch delivers small metadata directories and work navigation. Existing indexes are reused. See [current status](STATUS.md).

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

M0/M1 is delivered in v0.4.0, including source provenance, exact DOI associations, independent arXiv retrieval and exports. M2's small directories and hierarchy navigation are delivered in v0.4.1. See the [arXiv workflow](ARXIV.md) and [directory guide](DIRECTORIES.md).

After the selected version-details delivery, the next M2 batch is publisher/funder name catalogs and award-to-work associations. M3 covers reliable updates/deletions/redirects; M4 covers configurable additional work search scopes; M5 covers links and content downloads. See [the staged development plan](NEXT-STEPS.md) for dependencies and acceptance. These later stages are not implemented by the current release.

Semantic Scholar remains deferred. Crossref REST/public files are a separate optional bibliography source after DOI/version identity is established. Large new scopes follow the freshness contract; PDF/TEI retrieval remains later.

## Optional extensions

PDF/TEI downloads, broader non-DOI version association and calibrated matching, additional text
scopes/entity names, Parquet input, snapshot updates/deleted/merged IDs, semantic
retrieval, SDK and MCP are separate future tasks. They do not block this release
or require replacing the current engines. External A/D graph writing remains
outside the core project. Snapshot acquisition stays user-managed; private data,
engines, credentials and outputs remain outside Git.

The selected next delivery is conservative version details (v0.5.0 candidate), before the remaining M2 catalogs. See [the version workflow](VERSIONS.md); grouped search remains later work.
