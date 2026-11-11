# Agent development instructions

This file applies to the maintained source tree. Start here, then read only the
references relevant to the task. Human-facing documentation starts at
[the documentation index](docs/INDEX.md).

## Product and implementation boundaries

- Build a local OpenAlex metadata, citation and retrieval service from a
  user-downloaded snapshot. Provide a CLI, HTTP API and web UI.
- Snapshot acquisition is user-managed. Read a configured local path without
  modifying the source snapshot. Preserve raw records and unknown fields.
- ClickHouse is the metadata store; Manticore is the current optional lexical
  search engine. The default `metadata` profile does not require text indexes.
  `earth_sciences` is an optional example, not the project's discipline boundary.
- Internal deployments and community releases use the same maintained source
  and package. Put installation-specific choices in workspace configuration.
  Keep historical compatibility code separate from the supported workflow.
- Read [API coverage](docs/API-COVERAGE.md) before changing query behavior.
  Official-style routes do not establish full official API parity. Downloads,
  semantic search, version merging and snapshot synchronization remain extensions
  until implemented and qualified. External A/D graph tasks are outside this core.

## Where to work

- [Code map](docs/agents/CODE-MAP.md): entrypoints, modules, callers and tests.
- [Development contracts](docs/agents/DEVELOPMENT.md): persistent state, recovery,
  isolated validation and release checks.
- Resolve code resources relative to the package and mutable state relative to
  the configured workspace. Never hardcode an operator's IP, path or corpus size.
- Trace callers before editing shared behavior. Reuse query parsing, projections,
  batch readers, receipts and supervisors across CLI/API/web. Keep business rules
  out of frontend-only implementations and orchestration wrappers.
- Preserve separately callable processing modules and resumable pipeline stages.
  State inputs, outputs, prerequisites and acceptance conditions. Reuse compatible
  completed work; do not force a full import/reindex for an unrelated addition.
  Add no plugin framework or new engine without a concrete task requiring it.

## Data correctness and operations

- Keep imported metadata coverage, published text scope and indexed filter
  support distinct. Update `/capabilities` and UI availability with behavior.
- Preserve DOI ambiguity, missing source fields and unresolved relation targets.
  References are outgoing edges; citations reverse imported reference edges;
  related works use source relationships. Do not invent abstracts or references.
- Preserve route-specific authorship semantics. Name matches are candidates,
  not confirmed author identity, calibrated paper matches or automatic version merges.
- Freeze input identity, rules/projections and artifact dependencies. New inputs
  require a new generation/build as appropriate; never falsify hashes or receipts.
- Long jobs need durable progress, safe cancellation/restart, bounded retries for
  transient failures and surfaced terminal errors. Keep retries bounded; do not
  mask corruption, changed inputs or semantic errors as transient I/O failures.
- One owner holds workspace writers. Remote Pods use HTTP. Never let two database
  processes open one persistent ClickHouse directory or take over a live owner.
- Retain raw snapshots, private config, metadata, checkpoints, sealed index files,
  referenced parents, identity mappings and export dependencies. Before cleanup,
  establish dependencies and recovery needs; a Git checkout is not a data backup.
- Keep default caller QPS/concurrency rejection disabled. Tune actual workers,
  timeouts and memory from measured workloads; unlimited admission is not
  unlimited capacity. Preserve the deployment's explicit settings.

## Verification and delivery

- Test non-trivial behavior at its shared implementation and affected entrypoints.
  Use isolated synthetic workspaces. Live/native checks are explicit opt-ins;
  never point destructive tests at a production/shared workspace.
- Select checks by affected subsystem using the development reference. Report
  skipped checks and evidence limits. Do not change thresholds to obtain a pass.
- API changes must address CLI/web callers where relevant, authentication,
  supported query semantics, errors, docs and regression cases.
- Human-facing docs are paired English/Chinese. Update both versions in the same
  change, preserving concepts, commands and limits rather than literal phrasing.
  Agent instructions and `docs/agents/` are English-only operational references.
- Distinguish implementation, synthetic checks, full-data validation and actual
  deployment. Report version, scope, concurrency, background load and errors for
  performance claims. Reuse evidence only with its provenance and stated limits.
- Software release, dataset publication and service upgrade are separate actions.
  Preserve compatible data and rollback inputs; verify readiness and capabilities
  after a deployment. Cross-Pod evidence requires a real remote client.
- Git contains source, tools, packaged rules, templates and synthetic fixtures.
  Never commit keys, private config, snapshots, indexes, engines, environments,
  generated exports or machine-specific reports. Preserve code/data/engine licenses.
- Reference official repositories for contracts and optional integrations; verify
  current behavior and licensing before reuse. Do not import their production
  infrastructure or imply affiliation. Never rewrite released tags/assets for a
  documentation-only change.
