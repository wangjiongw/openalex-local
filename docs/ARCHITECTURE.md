# Architecture

`Snapshot → Metadata → Collections → Search → Verification → Publication → API/Web`

The source tree contains implementation, rules, templates and synthetic test
fixtures. A workspace owns configuration, raw snapshots, ClickHouse data,
immutable search snapshots, checkpoints, logs and generated reports.

The current implementation is being migrated gradually. Historical recovery
modules and `baseline/` are compatibility code, not an alternative default search
stack. No external knowledge-graph project is required by the community core.
The compatibility tree will be reduced only after its callers are migrated.

Only one writer may own a workspace. Remote clients use the HTTP API; two
ClickHouse processes must never open the same persistent data directory.
