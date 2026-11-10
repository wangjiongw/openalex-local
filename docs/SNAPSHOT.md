# OpenAlex snapshots

Official reference: https://help.openalex.org/access/snapshot/

The public S3 bucket is `s3://openalex`, anonymously readable using AWS CLI v2
with `--no-sign-request`. Check the manifests before downloading; a full format
is hundreds of GB compressed and requires additional database/index capacity.

Current formats contain equivalent entity records:

| Format | Files | Manifest |
| --- | --- | --- |
| JSON Lines | `data/jsonl/<entity>/updated_date=.../part_*.gz` | combined and entity `manifest.json` |
| Parquet | `data/parquet/<entity>/updated_date=.../part_*.parquet` | combined and entity `manifest.json` |
| Legacy JSON Lines | `data/<entity>/updated_date=.../part_*.gz` | entity `manifest` with `entries` |

Current manifests use `files` and combined `entities`; old manifests use
`entries`. `updated_date` partitions describe update time, not publication year.
Each JSONL line is one complete entity JSON object. Unknown fields must be
preserved. Missing abstracts and reference arrays must not be fabricated.

The toolchain reads user-provided local JSONL with old/new manifest adapters.
Parquet is documented and detected but is not an import format in this release.
Do not download both full copies unless you need both. The toolchain performs no
snapshot downloads and does not update the source directory.

## User-managed download

Install AWS CLI v2 separately and follow the [official snapshot guide](https://help.openalex.org/access/snapshot/).
Inspect the current combined manifest, release notes, download size and available
disk before transferring. Choose a new directory for each release. A full snapshot
is large; the following downloads the full JSONL prefix, not a small test fixture:

```bash
export SNAPSHOT=/absolute/path/to/openalex-snapshot
mkdir -p "$SNAPSHOT/data/jsonl"
aws s3 cp s3://openalex/RELEASE_NOTES.txt "$SNAPSHOT/RELEASE_NOTES.txt" --no-sign-request
aws s3 cp s3://openalex/LICENSE.txt "$SNAPSHOT/LICENSE.txt" --no-sign-request
aws s3 cp s3://openalex/data/jsonl/manifest.json "$SNAPSHOT/manifest.before.json" --no-sign-request
aws s3 sync s3://openalex/data/jsonl "$SNAPSHOT/data/jsonl" --no-sign-request
aws s3 cp s3://openalex/data/jsonl/manifest.json "$SNAPSHOT/manifest.after.json" --no-sign-request
cmp "$SNAPSHOT/manifest.before.json" "$SNAPSHOT/data/jsonl/manifest.json"
cmp "$SNAPSHOT/manifest.before.json" "$SNAPSHOT/manifest.after.json"
```

If the comparisons differ, the upstream release changed during acquisition. Do not
import a mixed release; finish a consistent download in a separately reviewed
release directory. Re-running AWS sync can resume transfers, but it is not a
release-freezing guarantee. Avoid `--delete` on directories containing other data.
Keep manifests, license, release notes and the relative shard directory structure.
Auxiliary deletion/merged-ID logs may be downloaded along with the prefix, but
this release does not apply them as incremental updates.

## Point the toolchain at the data

```bash
oa --workspace /path/to/workspace init --snapshot "$SNAPSHOT"
oa --workspace /path/to/workspace inventory
oa --workspace /path/to/workspace snapshot verify --deep
```

`--snapshot` is the directory containing `data/jsonl/manifest.json` or legacy
`data/<entity>/manifest`. A current JSONL prefix directory containing the combined
`manifest.json` directly is also accepted. Verification checks all shards listed
in the manifest; `--deep` checks gzip integrity, JSON and record counts. It can be
expensive on a full dataset. Do not present an unchanged full-release manifest
with only some of its shards: missing files are rejected. Intentional subsets
need a manifest describing their actual files and remain partial datasets.

No input data is copied into Git. The snapshot may be mounted read-only and live
outside the workspace. Changing input manifests after processing starts is
rejected; create a new generation for new input. Source licensing and project
code licensing are separate.
