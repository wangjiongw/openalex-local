# Contributing

Use a feature branch and include tests for non-trivial behavior. Run
`python -m pytest -q` after initializing an isolated workspace. Tests that need
real backends are explicitly opted in with `OA_LIVE_TESTS=1`; never aim them at a
shared production database. Never commit credentials, snapshots, indexes, binary
engines or machine-specific reports. Include the software version, platform,
input layout and a redacted reproducer in bug reports.

The opt-in native test (`OPENALEX_NATIVE_E2E=1 python -m pytest -q -s
tests/test_native_e2e.py`) downloads pinned engines into a temporary workspace,
builds synthetic data, restores, publishes and stops its owned services. Set
`OPENALEX_ENGINE_CACHE` for offline archives and `OPENALEX_ENGINE_PROFILE` for a
supported compatibility profile. It does not use your existing data. Native CI
runs independently on x86_64 and ARM64.

Contributions are provided under the project's MIT license. Preserve notices
for independently licensed material. Do not copy dependencies or unrelated
private-project code into this repository.
