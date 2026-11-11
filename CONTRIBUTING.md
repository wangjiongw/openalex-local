# Contributing

[中文](CONTRIBUTING.zh-CN.md) · [Documentation](docs/INDEX.md)

Use a feature branch and include tests for non-trivial behavior. Run
`python -m pytest -q` after initializing an isolated workspace. Tests that need
real backends are explicitly opted in with `OA_LIVE_TESTS=1`; never aim them at a
shared production database. Never commit credentials, snapshots, indexes, binary
engines or machine-specific reports. Include the software version, platform,
input layout and a redacted reproducer in bug reports.

## Keep implementation and documentation aligned

Internal deployments and community releases use one maintained source/package;
workspace settings carry local data and deployment choices. Reuse existing query,
batch-read, checkpoint, receipt and service-management implementations. Preserve
independent module entrypoints, resumability and compatible artifacts when changing
workflows. See [architecture](docs/ARCHITECTURE.md) for the processing model.

Human-facing documentation has paired English/Chinese versions. Update both in
the same change using natural explanations and aligned concepts; commands,
fields, limits and feature status must agree. Add language links and entries in
[the index](docs/INDEX.md). Development tools start at [AGENTS.md](AGENTS.md) and
read its English-only implementation references as needed.

## Choose verification by change

For docs-only edits, inspect links, concepts, commands and `git diff --check`;
prose needs no new behavioral tests. Behavior changes need affected tests and
the regular unit suite; engine/pipeline changes also need native checks.

The opt-in native test (`OPENALEX_NATIVE_E2E=1 python -m pytest -q -s
tests/test_native_e2e.py`) downloads pinned engines into a temporary workspace,
builds synthetic data, restores, publishes and stops its owned services. Set
`OPENALEX_ENGINE_CACHE` for offline archives and `OPENALEX_ENGINE_PROFILE` for a
supported compatibility profile. It does not use your existing data. Native CI
runs independently on x86_64 and ARM64.

Performance/recovery claims must identify the dataset, environment, exercised
scope, load and reused evidence. A successful binding, startup or single example
is not full qualification. See [validation](docs/VALIDATION.md).

Contributions are provided under the project's MIT license. Preserve notices
for independently licensed material. Do not copy dependencies or unrelated
private-project code into this repository.
