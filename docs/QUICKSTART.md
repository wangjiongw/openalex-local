# Native Linux quickstart

[中文](QUICKSTART.zh-CN.md) · [Documentation](INDEX.md)

Requires Python 3.10+, glibc Linux and sufficient persistent disk. Optional text
indexing also requires `dpkg-deb` (usually supplied by `dpkg`) to unpack Manticore.
AWS CLI is only a user-managed acquisition tool, not a processing dependency.
No root install of a database is required. Keep source and workspace in separate directories.

```bash
git clone https://github.com/wangjiongw/openalex-local.git
cd openalex-local
git checkout v0.3.0
python3 -m venv .venv
.venv/bin/pip install .
export PATH="$PWD/.venv/bin:$PATH"
export OPENALEX_HOME="$HOME/openalex-workspace"
oa init
```

For a packaged installation, download the wheel and `SHA256SUMS.txt` from the
[GitHub release](https://github.com/wangjiongw/openalex-local/releases/tag/v0.3.0),
verify its SHA-256, and run `python -m pip install /path/to/openalex_local_api-0.3.0-py3-none-any.whl`
in your virtual environment. No PyPI publication is required. The wheel contains
the UI and packaged rules; raw snapshots and engine binaries are separate.

`--workspace /absolute/path` overrides `OPENALEX_HOME`. The workspace defaults to
the current directory if neither is supplied. Private credentials are generated in
`config/local.json` with mode 0600. Do not add that file to Git.

## Bring a local snapshot

Download the desired OpenAlex release separately following the [snapshot guide](SNAPSHOT.md).
The project does not download snapshots. Preserve manifests and relative paths,
then specify the directory where your data already lives:

```bash
oa init --snapshot /absolute/path/to/openalex-snapshot
oa inventory
oa snapshot verify --deep
```

This updates the configured snapshot path without copying it. Read-only source
mounts are supported. Both legacy `data/<entity>/manifest` and current
`data/jsonl/manifest.json` inputs work; Parquet import is not supplied. Partial
snapshots need manifests describing their actual files and remain incomplete.

```bash
oa engines ensure  # ClickHouse only; no text engine required
# Or use matching pinned archives in a local cache:
oa engines ensure --offline --cache /path/to/engine-cache
```

Engines are hash/version pinned and installed inside the workspace. The default
ClickHouse LTS binaries target Linux amd64/arm64. Older ARM CPUs may need
`--profile armv8-compat`; its separately pinned ClickHouse archive must be available
in the offline cache if the upstream compatibility URL has changed. Changing a
profile does not overwrite an existing engine: use another workspace.

## Default: metadata and citation service

Edit local configuration before starting services. Defaults use 16 GiB resource
budget, 8 GiB ClickHouse limit, up to 8 CPUs, 4 import workers and API port 8088.
ClickHouse HTTP/TCP defaults to 18123/19000; optional Manticore HTTP uses 19308.
Configure `port`, `ch_url`, `ch_tcp_port` and, if used, `manticore_url` to avoid
collisions. Full snapshots require substantial persistent disk and may need
larger budgets. Benchmark storage before scaling workers.

```bash
oa pipeline plan --generation local_metadata
oa pipeline run --generation local_metadata
# The default run imports and audits metadata; publishing is explicit:
oa pipeline resume --generation local_metadata --until published
oa pipeline status --generation local_metadata
```

The default `metadata` profile reads all supplied entities, preserves complete
source records, and builds DOI/ID mappings and reference/citation relationships.
It neither classifies by discipline nor creates text indexes. Publication starts
the API with metadata access; title/name full-text search stays unavailable until
an optional index is built. `/readyz` can report a ready metadata-only deployment
while `/status` explicitly reports `search_ready: false`.

Resume reuses completed stages and rejects changed inputs/profile. No mandatory
24-hour wait applies. A new snapshot requires a new generation. Existing saved Earth plans must now
resume with `--profile earth_sciences`; their profile cannot be silently changed
in place.

## Optional example: Earth-science text search

Earth science is a supplied example subset, not a mandatory project scope.
Install the optional engine explicitly:

```bash
oa engines ensure --search
```

Starting from the snapshot with a new generation:

```bash
oa pipeline plan --generation earth_example --profile earth_sciences
oa pipeline run --generation earth_example --profile earth_sciences
oa pipeline resume --generation earth_example --profile earth_sciences --until published
```

This example adds Earth classification, versioned collection export, native
Manticore indexing, functional/load/recovery checks, and explicit index publication.
The default stop is `validated`. Full-snapshot search qualification uses the
existing 30-minute load policy; partial/demo data uses labelled short concurrent
smoke checks and does not establish full-data throughput.

To reuse an already imported `local_metadata` generation, invoke the existing
modules directly instead of importing another copy or changing its saved profile:

```bash
oa search-catalog local_metadata
oa earth-export local_metadata
oa earth-export-verify local_metadata
oa search-build local_metadata earth_example
oa search-validate local_metadata earth_example
oa search-publish local_metadata earth_example
```

Only the Earth-science text profile is supplied today. The metadata service is
independent of that choice; this release does not provide arbitrary-discipline
text indexing profiles.

A synthetic end-to-end trial can be generated without downloading real papers:

```bash
python -m openalex_api.demo "$OPENALEX_HOME/snapshots/synthetic"
oa init --snapshot "$OPENALEX_HOME/snapshots/synthetic"
# Run the default metadata pipeline, or opt into the Earth example above.
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
curl -H "X-API-Key: $OA_API_KEY" --get "$OA_URL/v1/works/lookup" \
  --data-urlencode 'identifier=10.99999/demo.2'
# Use a DOI present in your input; demo.2 exists only in the synthetic fixture.
```

Open `http://POD_IP:8088/` for the web UI; enter the same key. `/docs` lists API
routes. No per-key QPS or caller concurrency rejection is enabled by default;
worker counts and database timeouts still bound processing capacity. Publication
reports do not imply a cross-Pod test. For a published text index, from another Pod with access to the same
workspace (or a securely copied client config and fixtures), run:

```bash
oa verify --generation earth_example --base-url "$OA_URL" \
  --remote-client --output "$OPENALEX_HOME/reports/remote-verification"
```

Lifecycle: `oa start clickhouse`, `oa start api`, `oa status`, `oa stop all`,
`oa logs api`. Also run `oa start manticore` when restarting a deployment with a
published text index. A metadata-only deployment does not need that process. To exercise
recovery independently, use `oa restore --generation earth_example --output
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

Internal `0.1.0` deployments are not upgraded merely by checking out the release.
The source snapshot, metadata generation and published index each have their own
identity. Preserve their pointers, manifests, receipts, engine versions and parent
segments. See [release and operations](RELEASE.md) before adopting existing data.


`oa adopt --source-workspace /existing/workspace --generation GEN --build BUILD
--port 8089` binds a **fresh** workspace to a supported, frozen, already published
build. It creates its own API credentials and pointer, with source artifact
symlinks. Only explicitly registered semantically equivalent projections are
accepted. It cannot manage shared database processes or build new data. Start
its own API, run `oa verify`, and run `oa restore` separately before declaring an
adoption validated. Binding alone is not an acceptance result. It requires the
source metadata/search backends and paths to remain reachable; this is not a
portable backup or a full redeployment of the databases.

For scoped author/institution catalog expansion, durable batch exports and the export worker, see [SEARCH-EXPORT.md](SEARCH-EXPORT.md).
