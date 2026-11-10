# v0.2.0-rc.1 scope and operations

Git tag: `v0.2.0-rc.1`. Package/API version: `0.2.0rc1` (PEP 440). This is a
GitHub prerelease of the existing toolchain, not a new production dataset.
The internal `0.1.0` service and its published data remain separate.

## Frozen public workflow

- User downloads a consistent snapshot following [the format guide](SNAPSHOT.md).
- Read/verify local legacy/current JSONL; import preserved records, identifiers
  and forward/reverse citation relationships into ClickHouse.
- Publish the default metadata-only API/web service without Manticore.
- Optionally classify/export the Earth example and build, validate, restore and
  explicitly publish Manticore text indexes; reuse existing imported metadata.
- Query DOI/ID, full source records, references/citations/related works and supported
  indexed text/filters; inspect capabilities rather than assuming official parity.

No new routes, CLI flags, record schemas or index layouts are introduced by the
candidate version freeze. Full supported-query boundaries: [API coverage](API-COVERAGE.md).

## Supported command entrypoints

| Purpose | Commands |
| --- | --- |
| Workspace and input | `init --snapshot`, `inventory`, `snapshot verify [--deep]` |
| Native engines | `engines ensure`; add `--search` for Manticore; offline cache and documented platform profile |
| Lifecycle pipeline | `pipeline plan/run/resume/status --generation ...`; default `metadata`, optional `--profile earth_sciences` |
| Reuse imported metadata | `search-catalog`, `earth-export`, `earth-export-status`, `earth-export-stop`, `earth-export-verify` |
| Search build and publication | `search-build`, `search-validate`, `search-publish`, `search-status` |
| Existing-data checks | `adopt`, `verify`, `restore` (search restore uses the existing metadata server read-only) |
| Service operations | `start/stop/restart clickhouse`, `manticore` or `api`; `status`, `logs`, `doctor` |

Use `oa --workspace /absolute/workspace` before the command. Working examples
and required arguments are in [QUICKSTART](QUICKSTART.md), and `oa COMMAND --help`
shows the syntax. Raw `oa manticore` and incident-era commands are advanced or
compatibility entrypoints, not an alternative supported deployment workflow.
Old OpenSearch/Lucene migration, canary, diagnostic and `baseline/` code remains
because callers/tests still depend on it; the default service does not use it.
Do not run historical repair commands against an adopted or production dataset.

## Installation and persistent state

Install a tagged source checkout or the published wheel in a virtual environment.
Verify release artifacts against `SHA256SUMS.txt`. This candidate is distributed
through GitHub; no PyPI publication or bundled engine/data archive is required.

Keep these separately from Git:

- The original snapshot, manifests, license and release notes.
- Private workspace configuration, keys and engine installation identities.
- ClickHouse persistent data and metadata generation/checkpoints/audit receipts.
- Collection exports and all immutable index snapshots, manifests, receipts and
  referenced parent segments, including parents retained by a newer build.
- Local validation and operational reports.

A wheel or source checkout alone cannot restore a literature database. Git is
not a data backup. Do not delete published dependencies during source cleanup.

## Restart, reuse and rollback

Stop the old owner before moving a workspace. Only one host may own database
writers; never open the same ClickHouse directory from two processes/Pods. The
ownership marker is released manually only after confirming the old services
have stopped. Configure unused ports before starting an isolated workspace.

Restart ClickHouse, then Manticore if a search index is published, then the API.
Check `/healthz`, `/readyz`, authenticated `/status` and `/capabilities`. A
metadata-only deployment can be ready with `search_ready: false`. A running API
does not establish that every text-search scope is indexed.

Existing immutable indexes do not need rebuilding for compatible code/package
installation. `adopt` creates an isolated API binding to explicitly supported
frozen artifacts; it does not copy databases or qualify a new writable deployment.
Run functional checks against that API and keep the source installation available.

For release evaluation, keep the original API available and stop only the
candidate API to abandon the evaluation. For an independently deployed workspace,
retain the old software, configuration and pointers before an upgrade. Re-select
an old data/search version only after validating it against the selected code;
changing a Git tag alone is not a qualified rollback. Do not hand-edit published
receipts or bypass signature checks to make an old index appear compatible.

## Validation and known limits

The [validation record](VALIDATION.md) separates internal full-data evidence,
community native fixtures and candidate compatibility tests. Existing scale and
search-restore evidence is reused; candidate qualification does not reconstruct
or reindex the production corpus. Final query/load reports must be bound to
unchanged candidate code and unchanged production pointers/manifests.

- Text search covers only published indexes; Earth candidates are not all fields.
- Raw JSON preservation does not make every field an indexed filter. Some metadata
  statistics/sampling exist, but arbitrary text+aggregation combinations do not.
- Bibliographic text matching returns uncalibrated candidates. Version grouping,
  publication-first display and automatic duplicate merging are not supplied.
- Source abstracts/references/content may be missing. Index rebuilding cannot
  reconstruct data absent from the snapshot.
- PDF/TEI downloads, Parquet import, continuous updates, deleted/merged ID handling,
  semantic retrieval and SDK/MCP remain optional follow-up work.
- Existing search snapshots have passed full-data recovery checks; independent
  ClickHouse disaster recovery and physical Pod-loss recovery are not certified.
- Internal historical ClickHouse checksum failures remain an unresolved issue in
  that deployment's evidence. This source release does not claim their root cause
  was fixed; current checks must report any reproduced storage error.

No compulsory 24-hour observation applies. After candidate feedback, fixes and
validation, a stable release can be considered separately. No automatic internal
service cutover occurs when this prerelease is published.
