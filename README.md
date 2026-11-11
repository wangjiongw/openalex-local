# OpenAlex Local

[中文](docs/README.zh-CN.md) · [Documentation](docs/INDEX.md)

Build a local literature metadata and citation service from an OpenAlex snapshot
you have downloaded. The toolchain reads your local files, preserves complete
records, and serves a web UI and HTTP API. Optional indexes add efficient text retrieval.

**Stable release: v0.3.0. Package/API: `0.3.0`.**
Start with [native deployment](docs/QUICKSTART.md); see [validation](docs/VALIDATION.md)
for tested scope rather than assuming full live OpenAlex API parity.

## How it works

1. Download a consistent snapshot yourself using the [format/acquisition guide](docs/SNAPSHOT.md).
2. Point the toolchain at its local path; verify, import and audit records,
   DOI/ID mappings and forward/reverse citation relationships in ClickHouse.
3. Publish the metadata service: DOI/ID lookup, complete records and relation browsing work without text indexes.
4. Optionally run the Earth-science example, build and qualify Manticore indexes,
   then publish title, abstract, keyword and author-name search.
5. Optionally expand entity catalogs and export queries or DOI/ID lists through API, CLI or web.

The default `metadata` profile is discipline-independent. `earth_sciences` is the
only supplied text profile and an optional example. Raw metadata can resolve
out-of-scope papers even when their titles are absent from the published text index.
See [architecture and terminology](docs/ARCHITECTURE.md).

## What is included

- Official-style routes and local `/v1` extensions, supported combined filters,
  citation expansion, entity catalogs and a browser interface.
- Durable JSONL.gz/CSV/DOI/ID exports with cancellation, checkpoints and downloads.
- Resumable modules and pipeline, pinned native engines, explicit validation,
  publication and search recovery on Linux x86_64 and ARM64. Older ARM CPUs
  require the documented compatibility profile.
- One maintained package for internal and community installations; local
  datasets, keys, resources and pointers are workspace settings.

Snapshot paths can be outside the workspace. Data, indexes, credentials, engine
binaries, environments and reports stay outside Git. PDF/TEI downloads, automatic
version grouping, semantic retrieval and continuous snapshot updates remain
future work. Full imported metadata does not mean every field is indexed.

## Development and further reading

```bash
python3 -m venv .venv
.venv/bin/pip install -e . pytest
export OPENALEX_HOME="$(mktemp -d /tmp/openalex-dev.XXXXXX)"
.venv/bin/python -m openalex_api.cli init
.venv/bin/python -m pytest -q
```

The package is `openalex-local-api`, Python imports are `openalex_api`, and the
CLI is `oa`. Source code and runtime workspaces are separate.

Read the [documentation index](docs/INDEX.md), [feature inventory and official references](docs/FEATURES.md),
[API coverage](docs/API-COVERAGE.md), [query/exports](docs/SEARCH-EXPORT.md),
[operations](docs/RELEASE.md), [roadmap](docs/ROADMAP.md) and [contributing](CONTRIBUTING.md).
Development agents start at [AGENTS.md](AGENTS.md) and follow task-specific references.

## License and affiliation

Source code uses [MIT](LICENSE). OpenAlex data has its own CC0 license; obtain it
from the official source and retain its license/release notes. ClickHouse,
Manticore, AWS CLI and dependencies retain their own licenses. Engine binaries
and production datasets are not redistributed here. This independent community
project is not an official OpenAlex service.
