# Native Linux quickstart

Requires Python 3.10+, glibc Linux, `dpkg-deb` (usually supplied by `dpkg`),
AWS CLI v2 for snapshot downloads, and sufficient persistent disk. No root install
of a database is required. Keep source and workspace in separate directories.

```bash
git clone https://github.com/wangjiongw/openalex-local.git
cd openalex-local
python3 -m venv .venv
.venv/bin/pip install .
export PATH="$PWD/.venv/bin:$PATH"
export OPENALEX_HOME="$HOME/openalex-workspace"
oa init
```

`--workspace /absolute/path` overrides `OPENALEX_HOME`. The workspace defaults to
the current directory if neither is supplied. Private credentials are generated in
`config/local.json` with mode 0600. Do not add that file to Git.

## Input and engines

For a small real-data download (not a production-complete snapshot):

```bash
oa snapshot plan --target "$OPENALEX_HOME/snapshots/demo" --entities works --max-shards 1
oa snapshot download --plan "$OPENALEX_HOME/snapshots/demo/download-plan.json"
oa snapshot verify --target "$OPENALEX_HOME/snapshots/demo" --deep
```

For the full current JSONL release, omit `--entities` and `--max-shards`, choose a
new target, and review the plan's byte/record totals before downloading. Downloads
are anonymous, use bounded retries, and reuse objects only after checksum checks.
Repeating `snapshot download` resumes the frozen plan. A changed upstream manifest
requires a new plan/target rather than silently mixing releases.

For an existing snapshot use `oa init --snapshot /absolute/path/to/snapshot`.
This also updates an initialized workspace's snapshot path. Both legacy
`data/<entity>/manifest` and current `data/jsonl/manifest.json` inputs work.
A works-only shard can contain no Earth-science papers: use the synthetic fixture
below when testing the complete search pipeline.

```bash
oa engines ensure
# Or, with matching pinned archives in a local cache:
oa engines ensure --offline --cache /path/to/engine-cache
```

Engines are hash/version pinned and installed inside the workspace. The default
ClickHouse LTS binaries target Linux amd64/arm64. Older ARM CPUs may need
`--profile armv8-compat`; its separately pinned ClickHouse archive must be available
in the offline cache if the upstream compatibility URL has changed. Changing a
profile does not overwrite an existing engine: use another workspace.

## Process and publish

Edit local configuration before starting services. Defaults use 16 GiB resource
budget, 8 GiB ClickHouse limit, up to 8 CPUs, 4 import workers, API port 8088,
ClickHouse HTTP 18123/TCP 19000 and Manticore HTTP 19308. Configure `port`,
`ch_url`, `ch_tcp_port` and `manticore_url` to avoid collisions. Resource settings
are process budgets, not kernel reservations; full data needs much more disk and
may need larger memory budgets. Benchmark your storage before scaling workers.

```bash
oa pipeline plan --generation local_release
oa pipeline run --generation local_release --until validated
# Only publish after functional, load and fresh-engine restore checks pass:
oa pipeline resume --generation local_release --until published
oa pipeline status --generation local_release
```

Stages are metadata import/audit, Earth collections, native Manticore indexes,
validation/recovery, then explicit publication. Resume reuses completed stages and
rejects changed inputs/rules; new input requires a new generation. No mandatory
24-hour wait applies. Full-snapshot qualification uses the existing 30-minute
load policy. Partial/demo qualification uses short concurrency smoke checks and
is explicitly labelled; it does not establish full-data performance.

A synthetic end-to-end trial can be generated without downloading real papers:

```bash
python -m openalex_api.demo "$OPENALEX_HOME/snapshots/synthetic"
oa init --snapshot "$OPENALEX_HOME/snapshots/synthetic"
# Then run the pipeline above with engines installed.
```

## Query locally or across Pods

The API binds `host` (default `0.0.0.0`) and `port`. Ensure the Pod IP is reachable
and your network policy permits the port. Local `0.0.0.0` is a bind address, not a
client destination. Copy the key from your private config; do not paste it into
public reports or repository files.

```bash
export OA_URL=http://POD_IP:8088
# Supply OA_API_KEY securely from your local config.
curl "$OA_URL/readyz"
curl -H "X-API-Key: $OA_API_KEY" --get "$OA_URL/v1/works" \
  --data-urlencode 'search=climate' --data-urlencode 'search_field=title'
```

Open `http://POD_IP:8088/` for the web UI; enter the same key. `/docs` lists API
routes. No per-key QPS or caller concurrency rejection is enabled by default;
worker counts and database timeouts still bound processing capacity. Publication
reports do not imply a cross-Pod test. From another Pod with access to the same
workspace (or a securely copied client config and fixtures), run:

```bash
oa verify --generation local_release --base-url "$OA_URL" \
  --remote-client --output "$OPENALEX_HOME/reports/remote-verification"
```

Lifecycle: `oa start all`, `oa status`, `oa stop all`, `oa logs api`. To exercise
recovery independently, use `oa restore --generation local_release --output
/path/to/new-report`. Recovery starts an empty temporary Manticore engine, checks
snapshots/receipts and query results, and stops its own processes. It reuses the
existing metadata server read-only; it is not a ClickHouse disaster backup test.
Temporary restored files may require manual cleanup after reviewing reports.

Keep raw snapshots, workspace configuration/secrets, ClickHouse persistent data,
generation manifests/checkpoints, collection exports, search snapshots and parent
segments. Reusing a built index on a compatible Pod does not require reindexing.
Stop the original writer before relocating the workspace; never mount one
ClickHouse data directory into two writers. A hostname ownership marker refuses
automatic takeover; after verifying all old processes stopped, release
`var/run/workspace-owner.json` explicitly. Do not delete parent segments still
referenced by a published index. Retain independent backups.

## Existing installation adoption

`oa adopt --source-workspace /existing/workspace --generation GEN --build BUILD
--port 8089` binds a **fresh** workspace to a supported, frozen, already published
build. It creates its own API credentials and pointer, with source artifact
symlinks. Only explicitly registered semantically equivalent projections are
accepted. It cannot manage shared database processes or build new data. Start
its own API, run `oa verify`, and run `oa restore` separately before declaring an
adoption validated. Binding alone is not an acceptance result. It requires the
source metadata/search backends and paths to remain reachable; this is not a
portable backup or a full redeployment of the databases.
