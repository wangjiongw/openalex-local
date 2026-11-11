# Optional arXiv source

[中文](ARXIV.zh-CN.md) · [Documentation](INDEX.md)

arXiv is an independent optional source. OpenAlex remains the default. A published
arXiv release makes **every selected record** searchable, including unmatched
preprints. Counts, ranking and cursors apply to one source. Existing OpenAlex
metadata, references, indexes and raw JSON are retained unchanged.

## Obtain input

Download and unzip the metadata file from the
[Cornell arXiv dataset on Kaggle](https://www.kaggle.com/datasets/Cornell-University/arxiv/data).
Point the CLI at your local `arxiv-metadata-oai-snapshot.json`. Despite the suffix,
this file contains **one JSON object per line**, not an array. Uncompressed JSONL
and gzip JSONL are supported; the project does not download files or handle Kaggle
credentials. Metadata does not contain the papers' bibliography or full text.

The community path starts directly from that file. A pre-existing derived database
is optional: install `pip install 'openalex-local-api[arxiv-duckdb]'`, then use
`--input-format duckdb` with a local database containing `arxiv_metadata`. This
read-only compatibility adapter expects the fields described by
`arxiv_source.normalize`; it does not require the original processing environment
or external Parquet files. It cannot recover omitted parsed authors, full version
history or unknown original fields. Its preserved input is labeled
`derived_record`, never original Kaggle JSON. DuckDB is unnecessary for JSONL and
for the default OpenAlex workflow.

## Process and explicitly publish

Start the workspace's ClickHouse and Manticore services and publish a qualified
OpenAlex metadata generation first. Use a new source release name when input,
selection, processing code or the OpenAlex generation changes.

```bash
oa --workspace /path/to/workspace arxiv plan --release arxiv_2010_v1 \
  --generation your_generation --input /path/to/arxiv-metadata-oai-snapshot.json \
  --input-format jsonl --min-year 2010
oa --workspace /path/to/workspace arxiv run --release arxiv_2010_v1
oa --workspace /path/to/workspace arxiv status --release arxiv_2010_v1
oa --workspace /path/to/workspace arxiv validate --release arxiv_2010_v1
oa --workspace /path/to/workspace arxiv publish --release arxiv_2010_v1
```

`run` ends after validation; it does not change the active source. Use
`--until ingest|link|store|index|validate` to stop at a stage. Run long commands in a
persistent terminal. `stop` requests a stop at the next batch boundary; `resume`
removes that request and continues from durable receipts/checkpoints.

```bash
oa --workspace /path/to/workspace arxiv stop --release arxiv_2010_v1
oa --workspace /path/to/workspace arxiv resume --release arxiv_2010_v1
```

The selector uses **v1 first submission**, inclusive from 2010, across all arXiv
categories. Update/build dates never replace that date. Identical duplicates are
counted and deduplicated. Conflicting IDs and invalid required dates/fields fail
the job and prevent publication; correct the source in a new release. Invalid
optional DOI values remain in preserved input with `doi_status=invalid_source_value`.

## Modules and persisted dependencies

| Stage | Module | Input → output / acceptance |
| --- | --- | --- |
| Inventory | `arxiv_pipeline.plan` | Local path → immutable file hash, selector, code signature, OpenAlex proof |
| Normalize | `arxiv_source` | JSONL or read-only DuckDB → shared source record, retained input and explicit capabilities |
| Ingest | `arxiv_pipeline.ingest` | Source records → SQLite staging, unique IDs, transactional progress and duplicate counts |
| Link | `arxiv_pipeline.exact_links/link` | Batched existing DOI lookup/details → confirmed, ambiguous or unmatched evidence |
| Store | `arxiv_pipeline.store` | Staging → independent ClickHouse records/version links, batch reconciliation |
| Index | `arxiv_index`, existing `manticore_build` writer/seal | Every selected record → independent lexical index, verified receipts, sealed snapshot and restore proof |
| Validate/publish | `arxiv_pipeline` | Full metadata/index reconciliation → explicit independent active pointer |
| Serve/export | `arxiv_api`, existing API/export worker/web | Pinned source → separate queries, details, linked versions and downloads |

Persistent files are under `var/sources/arxiv/<release>/`: release manifest,
SQLite staging/checkpoints, index receipts, snapshots and validation report.
ClickHouse uses independent `oa_arxiv_<release>` tables. `var/arxiv-active.json`
selects the release and retains the previous pointer. Retain all dependencies of
published releases and export jobs. Source input is read-only and remains local;
none of these generated artifacts belong in Git.

Manticore startup restores the optional source separately. Failed arXiv recovery
marks that source unavailable and allows existing OpenAlex retrieval to start.
Changing the active OpenAlex generation invalidates the old source mappings;
create and validate a source release against the new generation.

## Query, versions and export

- `GET /v1/arxiv/works`: `search`, `search_field=all|title|abstract|author`,
  `author_search`, `year=YYYY|YYYY..YYYY`, `category`,
  `sort=relevance|submitted:desc|submitted:asc|id:asc`, `per_page`, signed `cursor`.
- `GET /v1/arxiv/lookup?identifier=arxiv:2304.02948`: normalized fields, preserved
  source input, provenance and exact version links. Version suffixes and old slash
  IDs are accepted. The public ID is always `arxiv:<base_id>`.
- `GET /v1/works/W123/versions`: confirmed linked source records and evidence.
- `/status` and `/capabilities`: optional `arxiv` readiness, scope and source release.
- `POST /v1/exports`: existing worker/formats, with `source: "arxiv"` and either
  `identifiers` or `query: {"route":"/v1/arxiv/works","params":{"search":"climate"}}`.
  Exports pin the source release, mappings and generation and use stable index ID
  order. Missing identifiers and missing DOI values remain reported.

The web page switches sources. OpenAlex filters are hidden/disabled for arXiv;
its year is first submission and category uses arXiv codes, not OpenAlex topics
or domains. Source cards keep their identity. Confirmed details prefer the
published OpenAlex record and provide a switch back to the preprint.

Only unique exact DOI evidence establishes links. Ambiguous matches are retained;
title similarity never merges records. An arXiv version array describes version
numbers/dates, not historical title/abstract/full-text contents. The published
record's reference list belongs to that record; it is never inserted into the
preprint's raw metadata. Exports use only an explicit source publication DOI;
no synthetic DOI fills a missing DOI field. An arXiv URL/license alone is not an
inferred OpenAlex OA status.

Online synchronization, Crossref, Semantic Scholar, full-text parsing/downloads
and mixed-source ranking are outside this workflow. Source implementation,
synthetic qualification, full-corpus publication and software deployment are
separate milestones; inspect your installed `/status` for availability.
