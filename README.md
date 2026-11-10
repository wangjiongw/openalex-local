# OpenAlex Local

Build a local literature search service from the OpenAlex snapshot: download,
validate, import, classify, index, verify, and serve a web UI and HTTP API.

**Community toolchain, v0.2.0 development.** JSONL download, resumable processing,
native engine installation, validation/recovery and explicit publication are
available. Start with the [native deployment guide](docs/QUICKSTART.md). Small
fixture checks establish pipeline correctness; production capacity and native
platform CI results must be assessed separately.

- Source records, DOI/ID lookup, references and citations use ClickHouse.
- Title, abstract, keyword and author-name searches use Manticore.
- Earth-science collections are the first supported indexing profile.
- Official-style OpenAlex routes coexist with local `/v1` extensions; full
  compatibility with the live official API is not claimed.
- Data, indexes, engine binaries, credentials and operational reports stay in a
  local workspace and are never part of this Git repository.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e . pytest
./oa init
.venv/bin/python -m pytest -q
```

The source package is currently named `openalex-local-api`, imports remain
`openalex_api`, and the CLI remains `oa`. Linux x86_64 and ARM64 are the target
platforms; support is declared only after native end-to-end checks pass.

See [validation status](docs/VALIDATION.md), [API coverage and limits](docs/API-COVERAGE.md), [snapshot formats](docs/SNAPSHOT.md), [architecture](docs/ARCHITECTURE.md),
[contributing](CONTRIBUTING.md), and [中文说明](docs/README.zh-CN.md).

## License and affiliation

Project source: [MIT](LICENSE). OpenAlex data has its own CC0 license; obtain data
from the official source and preserve its license and release notes. ClickHouse,
Manticore, AWS CLI and their dependencies retain their respective licenses. No
third-party engine binaries or production datasets are redistributed here.

This is an independent community project, not an official OpenAlex service.
