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

The first community toolchain implements JSONL, with old/new manifest adapters.
Parquet is documented and detected but is not an import format in this release.
Do not download both full copies unless you need both. New downloads freeze a
manifest and must not silently mix releases. Auxiliary deletion/merged-ID files
are preserved when available; incremental application is a later feature.

Download the official `LICENSE.txt` and `RELEASE_NOTES.txt` with the data. Source
code licensing and data licensing are separate. Runtime artifacts are local and
ignored by Git.
