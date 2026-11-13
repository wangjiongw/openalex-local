# Code map for development agents

Read [AGENTS.md](../../AGENTS.md) first. This reference is implementation
navigation, not the user guide. Confirm current definitions/callers with `rg`
before editing; file names alone do not imply the active execution path.

## Public entrypoints and execution

| Task | Implementation to trace | Relevant tests |
| --- | --- | --- |
| CLI/workspace selection | `openalex_api/cli.py` bootstrap, `community_cli.py`, `config.py`, `workspace.py` | `test_community.py`, `test_config_read.py`, `test_management.py` |
| Snapshot manifests and verification | `snapshot.py:inventory/verify`, `ingest.py:inventory` adapter | `test_snapshot.py`, `test_community.py` |
| Metadata processing and publication | `community_pipeline.py`, `ingest.py`, `backend.py`, `recovery.py` | `test_metadata_release.py`, `test_core.py`, `test_native_e2e.py` |
| Earth rules, classification and collections | `earth_rules.py`, `earth_catalog.py`, `earth_collections.py`, packaged JSON rules | `test_earth_rules.py`, `test_earth_collections.py` |
| Work projection and native index construction | `search_projection.py`, `manticore_build.py`, `manticore_pipeline.py`, `manticore_formats.py`, `manticore_membership.py` | `test_manticore_pipeline.py`, `test_manticore_native.py`, `test_native_e2e.py` |
| Small source directories | `directories.py`, `official.py:execute`, `/v1` listing in `api.py`; existing query/exports and web controls | `test_directories.py`, `test_native_e2e.py`, `check_web_layout.py` |
| Entity catalog expansion | `catalogs.py`, `catalog_build.py`, `identity_filters.py` | `test_catalog_build.py`, `test_identity_filters.py`, `test_native_e2e.py` |
| Query grammar and metadata predicates | `query.py`, `semantics.py`, `official.py`, `official_text.py` | `test_core.py`, `test_official.py`, `test_manticore.py` |
| Current search execution | `api.py`, `manticore_api.py`, `manticore_native_query.py`, `identity_filters.py` | `test_manticore_pipeline.py`, `test_identity_filters.py`, `test_native_e2e.py` |
| Identifier resolution and relation expansion | `api.py`, `backend.py`, `model.py`, `exact.py`, `exact_fast.py` | `test_backend_details.py`, `test_exact_fast.py`, `test_native_e2e.py` |
| Query exports | `exports.py`, API routes in `api.py`, CLI dispatch in `cli.py` | `test_exports.py`, `test_native_e2e.py` |
| Version relationships | `version_groups.py`, `versions_pipeline.py`, `versions_api.py`; CLI/API/web | `test_version_groups.py`, `test_native_e2e.py`, `check_web_layout.py`; see [contracts](VERSIONS.md) |
| Optional arXiv source | `arxiv_source.py`, `arxiv_pipeline.py`, `arxiv_index.py`, `arxiv_api.py`; existing CLI, API and export worker | `test_arxiv.py`, arXiv portion of `test_native_e2e.py`, browser source-switch checks |
| UI | `static/app.js`, `static/index.html`, `static/app.css` | `scripts/check_manticore_browser.py`, browser acceptance described in validation docs |
| Services, ownership and resources | `manage.py`, `workspace.py`, `budget.py`, `config.py`, `engines.py` | `test_management.py`, `test_capacity_guard.py`, `test_community.py` |
| Qualification, restore and publication | `acceptance.py`, `operations.py`, `manticore_qualification.py`, `manticore_runtime.py`, `search_versions.py`, `proofs.py` | `test_manticore_full_restore.py`, `test_native_e2e.py`, `test_release_continuation.py` |

Module names in this table are relative to `openalex_api/`; test names are
relative to `tests/`. `oa` is a convenience script; the installed entrypoint is
`oa = openalex_api.cli:main` in `pyproject.toml`.

`community_pipeline.py` coordinates `metadata → collections → index → validated
→ published`; the metadata profile uses only `metadata → published`. Read its
`plan`, `run` and `publish` before altering stage completion or recovery. Catalog
expansion and query exports have independent commands/workers and are not extra
implicit stages in that pipeline. Do not document them as automatic stages.

See [arXiv contracts](ARXIV.md) for the optional source and its ordered evidence checkpoints.

## Query changes: trace both contracts

1. `api.py` selects official-style routes or local `/v1` behavior.
2. `official.py` can execute supported metadata predicates directly in ClickHouse;
   `official_text.py` and `query.py` construct indexed query bodies.
3. `manticore_api.py` resolves published segments and executes the native request.
   `identity_filters.py` narrows mandatory identity candidates and can use the
   same engine's HTTP SQL endpoint for docid lookup.
4. `exports.py:prepare/rows` consumes those query semantics with a pinned build
   and stable ID ordering. Check exports when altering filters or result shape.
5. Update capabilities and UI controls together with supported behavior.

For `/v1` positive author+institution conditions, preserve same-authorship
coupling. Official-style routes retain work-level intersection. Candidate
prefiltering must not replace the original predicate or truncate results above
the candidate bound. SQL/native adapters need parity for ranking, multi-value
negation, counts and scrolling.

## Packaged resources and compatibility code

Runtime rules and engine/projection identities live in `openalex_api/resources/`;
matching editable/example files also exist in `config/`. Inspect consumers and
synchronization tests before changing either. UI files and JSON resources must
be included in the installed wheel, not only available from a source checkout.

`baseline/`, old OpenSearch/Lucene code, storage-diagnostic Java tools and
incident-era commands remain because compatibility/recovery callers and tests
exist. They are not the supported default search stack. Trace actual imports
before deleting or routing new work through them. In particular, legacy
`storage.py` verification is not the current Manticore acceptance policy.

See [development contracts](DEVELOPMENT.md) for persistent artifacts and safe checks.
