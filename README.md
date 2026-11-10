# OpenAlex Local

Use an OpenAlex snapshot you have downloaded to build a local metadata and
citation service with a web UI and HTTP API. Point the toolchain at its local
path, validate and import it, then optionally classify a subset and build text
search indexes.

**Development: 0.3.0rc1; latest published candidate: v0.2.0-rc.1.** Snapshot acquisition is user-managed;
this project provides [download and format guidance](docs/SNAPSHOT.md), local
readers, resumable processing, native engine installation, validation/recovery
and deployment. Start with the [native deployment guide](docs/QUICKSTART.md).

- The default pipeline imports and audits source records, DOI/ID mappings and
  reference/citation relationships in ClickHouse, independently of discipline.
- Text indexing is optional. The included **Earth-science example** demonstrates
  subset classification, collection export and Manticore indexing; it does not
  restrict the metadata service to Earth science. Other text indexing profiles
  are not supplied in this release.
- Title, abstract, keyword and author-name text searches require a published
  text index; DOI/ID lookup and citation expansion do not.
- Official-style OpenAlex routes coexist with local `/v1` extensions; full
  compatibility with the live official API is not claimed.
- Snapshots may stay at any readable local path. Workspace data, indexes,
  binaries, credentials and reports are never part of this Git repository.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e . pytest
./oa init
.venv/bin/python -m pytest -q
```

See [release scope, supported commands and operations](docs/RELEASE.md).
The published candidate freezes its existing feature set. This development branch adds [query, entity catalogs and durable batch exports](docs/SEARCH-EXPORT.md); production-scale catalog acceptance is recorded separately. Optional content downloads,
version grouping and continuous snapshot updates remain future work.

The source package is named `openalex-local-api`, imports remain
`openalex_api`, and the CLI remains `oa`. Native metadata and Earth-example workflows pass on Linux x86_64 and ARM64;
older ARM CPUs require the documented compatibility engine profile.

See the [功能清单与扩展规划](docs/FEATURES.zh-CN.md), [validation status](docs/VALIDATION.md), [API coverage and limits](docs/API-COVERAGE.md), [snapshot formats](docs/SNAPSHOT.md), [architecture](docs/ARCHITECTURE.md),
[contributing](CONTRIBUTING.md), and [中文说明](docs/README.zh-CN.md).

## License and affiliation

Project source: [MIT](LICENSE). OpenAlex data has its own CC0 license; obtain data
from the official source and preserve its license and release notes. ClickHouse,
Manticore, AWS CLI and their dependencies retain their respective licenses. No
third-party engine binaries or production datasets are redistributed here.

This is an independent community project, not an official OpenAlex service.
