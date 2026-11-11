# Search and durable batch exports

[中文](SEARCH-EXPORT.zh-CN.md) · [Documentation](INDEX.md)

Stable version v0.3.1 (package/API `0.3.1`) retains the qualified entity
catalogs and durable exports from v0.3.0-rc.1. Catalog construction, physical recovery and mixed
query/export qualification passed on an existing dataset; see [validation](VALIDATION.md).
Each installation must build, qualify and publish its own catalogs. Data,
private configuration, jobs and output files stay outside Git.

## Queries and the web UI

Use `/works`, `/authors`, `/institutions`, `/sources`, `/topics` and the
corresponding `/v1` routes. Title, abstract, keyword and author-name searches can
combine with supported year, OA, author/institution identity, source, topic,
type and language predicates. Inspect `/capabilities` for the actual contract.
The UI adds author identity, institution and language controls, query URLs,
result selection and an export-job panel.

Positive author/institution conditions on `/v1` must hold within the same
authorship. Official-style routes preserve work-level intersection. Name search
does not establish identity, and selecting an author ID does not merge namesakes.
Entity-name searches include aliases from source records. The author catalog
contains authors linked to published Earth candidates; the institution catalog
covers imported institutions. Identifier lookup has a different scope from text search.

## Create an export

For v0.3.1 keyless internal deployments (`auth_required: false`), the same export
and file endpoints work without credentials. The default remains authenticated;
see [deployment settings](QUICKSTART.md#optional-keyless-internal-deployment).

All endpoints use the existing API authentication:

```http
POST /v1/exports
Content-Type: application/json
X-API-Key: <local-key>
```

```json
{
  "query": {
    "route": "/v1/works",
    "params": {
      "search": "climate",
      "search_field": "title",
      "filter": "year:2020..2026,is_oa:true"
    }
  },
  "formats": ["jsonl.gz", "csv", "doi.txt", "ids.txt"]
}
```

Alternatively, replace `query` with `"identifiers": ["W1", "10.1234/example"]`.
The two inputs are mutually exclusive. `query.route` accepts only `/v1/works`
and `/works`, never external URLs. Export queries exclude pagination, cursors,
grouping, sampling and authentication parameters. They export the complete
matching set in stable ID order, independently of UI ranking. Official-style
metadata-only queries cover imported metadata; text queries and `/v1` queries
cover the published index.

- Optional `select`: comma-separated top-level original fields for JSONL;
  omit it to preserve the complete source record.
- Optional `csv_fields`: column-name array. Defaults to
  `id,doi,title,publication_year,authors,abstract,abstract_status,source,is_oa,oa_status,oa_url,cited_by_count`.
- CSV multi-value cells contain JSON. Authors, abstract, source and OA columns
  are convenient projections of source fields. Missing/invalid abstracts remain
  blank; `abstract_status` distinguishes missing/invalid/present. Full JSON does
  not rewrite the original record.
- Works without a DOI are retained and have separate missing-doi chunks.
  Invalid, not-found and ambiguous list inputs are reported in the manifest;
  candidates are not selected automatically.
- Complete JSON includes original reference IDs. This version does not export
  every referenced target as an additional full record.

| Endpoint | Behavior |
| --- | --- |
| `GET /v1/exports?offset=0&limit=25` | List jobs |
| `GET /v1/exports/{id}` | Read progress, errors and committed files |
| `POST /v1/exports/{id}/cancel` | Cancel after committing the current checkpoint |
| `POST /v1/exports/{id}/resume` | Verify committed output and resume; cancel running jobs first |
| `GET /v1/exports/{id}/files/{name}` | Download committed chunks, or manifest.json after completion |

Jobs live in the workspace's `exports/query/`. `exported` counts records in
committed chunks; it can remain unchanged while a large chunk is being written.
Recovery continues from committed checkpoints. Every job pins its generation,
applicable search build, query, fields and formats. Shard receipts record counts,
read positions, hashes and files; recovery does not use a browser cursor.
The default chunk target is about 100,000 records or 256 MiB of uncompressed
output, applied at batch boundaries; a single large record can exceed the target.
States are queued/running/paused/cancelled/failed/completed. Resource shortage
pauses the affected job; persistent files are retained by default. Never edit
receipts or remove data/index versions still referenced by jobs.

## CLI and worker lifecycle

```bash
oa --workspace /path/to/workspace start export-worker
oa --workspace /path/to/workspace export create --request request.json
oa --workspace /path/to/workspace export list
oa --workspace /path/to/workspace export status --job <id>
oa --workspace /path/to/workspace export cancel --job <id>
oa --workspace /path/to/workspace export resume --job <id>
```

`start all` includes the export worker. Restarting only the API does not stop
exports. By default two jobs run concurrently. Catalog extraction defaults to
at most eight processes and 10,000 works per batch, configurable through
`catalog_workers` and `catalog_batch_rows`. Export controls include
`export_workers`, `export_batch_rows` and `export_shard_rows`. No extra query
QPS quota is introduced. Jobs queue while the worker is stopped; metadata API
readiness is independent of worker availability.

Read-only adoption continues to use source engines/frozen data; new exports
write to the adopted workspace, not the source workspace's export directory.

## Expand catalogs while reusing works

```bash
oa --workspace /path/to/workspace catalogs build --generation <generation> --build <new_build> --parent <published_earth_build> --background
oa --workspace /path/to/workspace catalogs status --generation <generation> --build <new_build>
oa --workspace /path/to/workspace catalogs validate --generation <generation> --build <new_build>
oa --workspace /path/to/workspace catalogs publish --generation <generation> --build <new_build>
```

Construction adds the full imported institution catalog, extracts and
deduplicates author IDs from frozen work segments, and builds author-name
indexes in parallel over disjoint ID ranges. Each block prefetches its next
batch and maintains a checkpoint. Sealed prefixes are reused; the parent
process seals snapshots sequentially to limit contention. Classification and
the complete works build are not repeated.

The same works produce recoverable author/institution-ID-to-work-ID candidate
mappings in a derived ClickHouse table within the generation. Queries and
exports narrow mandatory positive identity conditions first, then apply the
original authorship predicate. Large candidate sets use the existing Manticore
HTTP SQL endpoint to force docid lookup rather than an optimizer full scan;
API counts and pagination tokens retain their meaning.

An identity exceeding 250,000 candidates, or negative-only/optional OR
conditions, keeps exact scanning without truncating results and may be slower.
The derived table and `identity-progress.json` are persistent dependencies;
restore them or rebuild in a new build if missing. Added snapshots retain
absolute dependencies on original segments: do not delete parent indexes.
Read `progress.json` in the new build directory for progress and estimates.
Retry failed construction with the same parameters; receipts can replay missing
unsealed data.

Qualification reuses parent full-index recovery evidence and physically checks
and restores the added segments. Complete snapshots receive common-query,
entity-query and 30-minute mixed query/export acceptance before explicit
publication. Synthetic native checks do not establish real-scale performance.
There is no compulsory 24-hour observation wait.

This version does not include the full all-discipline author-name catalog, PDF
downloads, automatic version association, semantic retrieval, arbitrary-discipline
profiles, snapshot updates or independent ClickHouse disaster recovery.
