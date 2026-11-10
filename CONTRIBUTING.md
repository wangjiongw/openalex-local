# Contributing

Use a feature branch and include tests for non-trivial behavior. Run
`python -m pytest -q` after initializing an isolated workspace. Tests that need
real backends are explicitly opted in with `OA_LIVE_TESTS=1`; never aim them at a
shared production database. Never commit credentials, snapshots, indexes, binary
engines or machine-specific reports. Include the software version, platform,
input layout and a redacted reproducer in bug reports.

Contributions are provided under the project's MIT license. Preserve notices
for independently licensed material. Do not copy dependencies or unrelated
private-project code into this repository.
