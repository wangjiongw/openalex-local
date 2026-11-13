# Documentation

[中文](INDEX.zh-CN.md) · [Project overview](../README.md)

OpenAlex Local turns a locally stored snapshot into a metadata and citation
service, with optional text indexes. Start with the user journey below; you can
operate the metadata service before choosing a text-search scope.

## Read in order

| Your question | Guide |
| --- | --- |
| What does the project do, and what is optional? | [Overview](../README.md), [feature inventory](FEATURES.md) |
| What do snapshot, workspace, generation and build mean? | [Architecture and concepts](ARCHITECTURE.md) |
| Which files should I download and where should they go? | [Snapshot formats and acquisition](SNAPSHOT.md) |
| How do I get a local service running? | [Native Linux quickstart](QUICKSTART.md) |
| Which searches and relationships can I query? | [Query coverage](API-COVERAGE.md); running service `/docs` and `/capabilities` |
| How do I export a full result set or DOI list? | [Search and durable exports](SEARCH-EXPORT.md) |
| How do I retain, restart, move or roll back a deployment? | [Release and operations](RELEASE.md) |
| What has actually been tested? | [Validation evidence and limits](VALIDATION.md) |
| What comes next and which official projects are useful? | [Roadmap](ROADMAP.md), [official references](FEATURES.md#3-official-repositories-and-extension-candidates) |
| How can I contribute? | [Contributing](../CONTRIBUTING.md), [changelog](../CHANGELOG.md) |

The [next development plan](NEXT-STEPS.md) describes source enrichment, freshness,
Earth entities, wider scopes and later content downloads, separately from released features.

## Releases and development tools

- Current stable release: [v0.4.1](releases/v0.4.1.md). Previous stable: [v0.4.0](releases/v0.4.0.md).
- [Current project status](STATUS.md): delivered workflow, known limits and maintenance boundary.
- Release history: [v0.3.0-rc.1](releases/v0.3.0-rc.1.md), [v0.2.0-rc.1](releases/v0.2.0-rc.1.md).
- Human-facing documents have English/Chinese counterparts with language links.
  Code, commands, API field names and identifiers use the same spelling in both.
- [AGENTS.md](../AGENTS.md) and [agent implementation references](agents/CODE-MAP.md)
  are English-only instructions for development tools. They cover code navigation
  and execution constraints rather than replacing these explanatory guides.
- Source is public; snapshot data, credentials, indexes, engines, exports and
  deployment reports remain in local workspaces. A source release is not a database backup.

- [Optional arXiv input, processing and search](ARXIV.md)

- [Small entity directories and work navigation](DIRECTORIES.md).

- [Proposed OpenAlex version groups](VERSION-GROUPS-PLAN.md).
