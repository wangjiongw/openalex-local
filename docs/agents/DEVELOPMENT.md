# Development contracts and checks for agents

Read [AGENTS.md](../../AGENTS.md) and the relevant [code-map row](CODE-MAP.md).
This file specifies how to inspect and verify a change without operating on a
user's dataset. User-facing procedures are in [the quickstart](../QUICKSTART.md).

## State and identity contracts

| State | Inspect implementation | Invariant |
| --- | --- | --- |
| Workspace and credentials | `config.py`, `workspace.py`, `manage.py` | `--workspace` overrides `OPENALEX_HOME`; config is loaded relative to the selected workspace; only its owner starts writers |
| Source manifests | `snapshot.py`, `ingest.py:create_generation` | source path is read-only input; actual files must match the declared release/subset; freeze manifest identity |
| Metadata generation | `var/generations/<generation>/generation.json`, `ingest.py`, `proofs.py`, `recovery.py` | preserve full records, identifier ambiguities and unresolved targets; verify source/identifier/forward/reverse consistency before release |
| Pipeline | `community_pipeline.py`, generation `pipeline.json` | resume only the recorded input/profile; completed stages are reusable; changed input requires a new identity |
| Search build | `search_versions.py`, `manticore_build.py`, `manticore_qualification.py` | inspect actual manifests, projection signatures, receipts and parent dependencies; build completion is not publication |
| Catalog additions | `catalogs.py`, `catalog_build.py`, `identity_filters.py` | reuse frozen works; preserve membership/missing counts, replay identity and same-authorship semantics |
| Exports | `exports.py`, `exports/query/<job>/` | immutable plan, durable state, verified shard receipts; only committed chunks contribute to exported progress |
| Active pointers | `config.py:active`, `community_pipeline.py:publish`, engine-specific publish functions | software, metadata and search versions are distinct; cutover requires valid evidence and a recoverable previous state |

Search state lives below `var/generations/<generation>/searches/<build>/`.
Catalog `identity-progress.json` and its derived ClickHouse table are persistent
search dependencies. A child build can reference parent files by absolute path;
copying only the newest directory does not produce a portable backup. Query
exports can pin older generations/builds after a newer one is published.
Inspect references before any cleanup; never rewrite seals to accept changed files.

For read-only adoption, inspect `adoption.py` and packaged projection compatibility
rules. Adoption binds supported frozen source artifacts and shared backends;
its new export jobs write to its own workspace. Do not confuse it with a database
copy or writable ownership transfer.

Transient shared-filesystem absence is possible. Use the existing bounded read
helpers and atomic-write/locking patterns where applicable. Exhausted retries
must expose a failure and preserve recoverable progress. Do not suppress missing
persistent dependencies or infer a network-storage root cause without evidence.

## Local checks in an isolated workspace

Run from the maintained source checkout, with a development virtual environment:

```bash
python3 -m venv .venv
.venv/bin/pip install -e . pytest build
export OPENALEX_HOME="$(mktemp -d /tmp/openalex-dev.XXXXXX)"
.venv/bin/python -m openalex_api.cli init
.venv/bin/python -m pytest -q
.venv/bin/python -m build
```

Never substitute a real service workspace in these commands. Inspect environment
pins such as `OPENALEX_GENERATION`, `OPENALEX_SEARCH_BUILD` and
`OPENALEX_SEARCH_VALIDATION` before diagnosing a surprising result. Use a fresh
process when changing workspace; `config.ROOT` is set at import time.

Choose extra checks from the affected subsystem:

- **Docs only:** inspect English/Chinese parity, local links/anchors, examples
  against CLI `--help`, public-content hygiene and `git diff --check`. Do not add
  behavioral tests or run a full-data reindex to validate prose.
- **Query/identity logic:** exercise both route contracts, missing/null fields,
  negatives, optional OR, same-authorship, totals, ranking and scrolling. Verify
  exports use the same filters. Start with the mapped unit tests.
- **Import/projection/receipt changes:** verify unknown-field preservation,
  missing-target behavior, idempotent resume, changed-input rejection and
  signature compatibility. Use native fixtures before real-data qualification.
- **Exports:** verify input validation/authentication, source/build pinning,
  CSV/JSON projections, committed counts, cancellation/resume, damaged-receipt
  refusal, missing/ambiguous identifiers and download hashes.
- **Web:** exercise affected API-backed controls, URL restoration, error states,
  job/download behavior and desktop/mobile layout. Use synthetic or isolated data.

The opt-in native fixture downloads pinned engines and owns temporary services:

```bash
OPENALEX_NATIVE_E2E=1 .venv/bin/python -m pytest -q -s tests/test_native_e2e.py
```

`OPENALEX_ENGINE_CACHE` supplies cached archives;
`OPENALEX_ENGINE_PROFILE=armv8-compat` selects the documented older-ARM profile.
`OA_LIVE_TESTS=1` enables legacy live checks and is not a generic safe test mode.
Inspect each test's targets before opting in. Existing CI runs unit/build and
native jobs on Linux x86_64 and ARM64. For packaging changes, also test the wheel
from outside the source tree so imports cannot silently use checkout files.

## Qualification and delivery

Synthetic native tests prove the exercised flow, not corpus recall, full-data
latency or independent database disaster recovery. Use the current qualification
code and frozen query fixtures for a data release. Preserve evidence bindings
and report reused parent-restore evidence explicitly. A URL reachable locally
does not prove cross-Pod operation; that check must run from another client Pod.

Preserve existing deployment settings and old software/pointers before upgrades.
Check `/healthz`, `/readyz`, authenticated `/status` and `/capabilities`; metadata
readiness can coexist with unavailable text search. An export worker restart
is independent of an API restart. No mandatory 24-hour idle wait applies.

Before committing: inspect `git diff`, track only intended paths, check licenses
and both language versions, and exclude workspace data, secrets, engines and
machine reports. Do not commit production fixture extracts as synthetic examples.
Document checks run/skipped, resulting behavior, measured limits and rollback
needs. Never push all local tags; preserve existing released tags and artifacts.
