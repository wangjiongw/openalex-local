# OpenAlex Local feature inventory

[中文](FEATURES.zh-CN.md) · [Documentation](INDEX.md)

This project turns a user-downloaded public OpenAlex snapshot into a local
metadata, citation and retrieval service with CLI, HTTP API and web interfaces.
It preserves source records and builds optional text indexes. Earth science is
the supplied example scope, not the boundary of the metadata service.

This inventory combines internal scale evidence, community implementation and
the responsibilities of official repositories. See [search/exports](SEARCH-EXPORT.md)
for the `0.3.2` additions and [release operations](RELEASE.md) for the supported
commands. Actual catalog availability depends on the published build and
`/capabilities`. Extension candidates below are not delivered integrations.

## 1. Status and boundaries

- **Implemented:** code exists; this does not establish scale qualification for every input or query combination.
- **Validated:** internal operation, delivery or community-test evidence exists, only for its recorded version and scope.
- **Partial:** implemented with remaining coverage, query, performance or quality limits.
- **Future:** not delivered; references and intended results are provided below.
- The historical internal baseline was `0.1.0`; the community toolchain and upgraded internal service use `0.3.2`. Deployments use the same package with local data, configuration and rollback inputs. A community release does not automatically upgrade other installations.
- The internal Earth candidate index published and qualified 76,850,149 works, 4,516 topics and 280,687 sources. Search snapshots total about 501 GiB; fresh-engine recovery and cross-Pod checks each passed 784 functional cases.
- Both metadata-only and optional Earth native workflows pass Linux x86_64/ARM64 CI. Community synthetic tests do not replace full production performance evidence.
- **Existing index construction and recovery are reusable assets.** Future work extends interfaces, scopes, formats and compatibility rather than treating validated processing as unimplemented.

## 2. From a public snapshot to a local service

```text
Official public snapshot
    | User downloads following official guidance; no project downloader
    v
Local path → Inventory/verify → Import complete metadata and citation edges
                                  |
                                  + Default: DOI/ID, full records, relations → API/web
                                  |
                                  + Optional: select scope → Build/qualify/publish index
                                                                |
                                                                + Title, abstract and author-name retrieval
```

Classification and collection export are optional scope-processing modules.
Text indexes are optional for metadata access and necessary for efficient
indexed text retrieval. Only the `earth_sciences` automated example is supplied;
generic all-discipline/arbitrary-discipline profiles are not delivered.

### 2.1 Acquisition guidance and inputs

| ID | Capability | Status and boundary |
| --- | --- | --- |
| A01 | Official download, formats, directories, licensing and manifests | Guidance supplied; acquisition is user-managed; see [snapshots](SNAPSHOT.md) |
| A02 | Select local input | Implemented: `oa init --snapshot /path/to/snapshot`; source can be outside the workspace and read-only |
| A03 | Legacy/current JSONL adapters | Implemented: legacy `data/<entity>/manifest`, current `data/jsonl/manifest.json`, or the direct JSONL prefix root |
| A04 | Inventory and integrity | Implemented: entities, shards, counts, sizes and missing files; deep gzip/JSON/actual-count checks |
| A05 | Input identity binding | Implemented: manifest fingerprint bound to generation; changed inputs reject silent resume; subsets remain partial |
| A06 | Parquet input | Future: detected and reported as unsupported; upstream availability does not establish local import support |
| A07 | Snapshot refresh | Future: processing uses frozen input; no automatic upstream acquisition or update application |

### 2.2 Metadata, identifiers and citations

| ID | Capability | Status and boundary |
| --- | --- | --- |
| B01 | Complete entity records | Implemented: read entities listed in supported manifests, preserve full JSON and unknown fields, do not fabricate missing information |
| B02 | Common bibliography | Implemented: DOI, title, authors/institutions, year, source, topics, keywords, OA and locations, as present in source records |
| B03 | Abstract access | Implemented: preserve `abstract_inverted_index`, reconstruct text for UI/lists; missing source abstracts are not filled automatically |
| B04 | Identifier mappings | Implemented: DOI/OpenAlex ID; relevant entity routes also support built PMID, PMCID, ORCID, ROR and ISSN mappings |
| B05 | Batch metadata lookup | Implemented: `POST /v1/works/batch`, found/not_found/ambiguous; currently at most 100 inputs per request |
| B06 | Forward/reverse citation tables | Implemented and validated: references/citations from source edges, expandable into readable DOI/title/full records |
| B07 | Missing references and counts | Basic distinctions implemented: unresolved IDs retained; reverse-edge totals need not equal source `cited_by_count`; empty arrays do not prove no real references |
| B08 | Source related-work lists | Implemented: expand `related_works`; not newly computed semantic recommendations |
| B09 | Import checkpoints and audits | Implemented and validated: resume, source-record consistency, identifiers and bidirectional edge audits; new input needs a new generation |
| B10 | Deleted/merged IDs | Future: apply `deleted_ids`/`merged_ids`, redirects and reference edges; distinct from grouping paper versions |

### 2.3 Optional collections and scope processing

| ID | Capability | Status and boundary |
| --- | --- | --- |
| C01 | Broad Earth example | Implemented and validated: combine topic/discipline hierarchy, concepts, journals, text evidence and constrained author expansion |
| C02 | Separate literature groups | Implemented: core journals, broad/interdisciplinary Earth, and author-evidence expansion; not every candidate is a strict Earth paper |
| C03 | Literature list export | Implemented and validated: JSONL.gz/CSV with DOI, title, authors, abstract, OA, inclusion reasons and missing-value flags |
| C04 | Reuse imported metadata | Implemented: classify/build on an existing generation without importing another snapshot copy |
| C05 | Arbitrary scope profiles | Future: explicit all-field/topic/journal/user-ID inputs reusing projections/builders, without replacing the engine |
| C06 | Rerun official classifiers | Not required for the core: source labels already exist; needed only for new records or deliberate reclassification |

### 2.4 Text indexes and publication

| ID | Capability | Status and boundary |
| --- | --- | --- |
| D01 | Search projections | Implemented: derive text, identifiers and common filters; index is an acceleration layer, not a replacement for raw JSON |
| D02 | Manticore construction | Implemented and internally validated at scale; current retrieval does not depend on old Lucene/OpenSearch indexes |
| D03 | Chunks, batches and recovery | Implemented: receipts, verification, progress/resume and segment reuse; continue with frozen data/layout |
| D04 | Build resources | Implemented: concurrency, CPU/memory budget and disk reserve controls; throughput depends on scale and storage |
| D05 | Persistent search snapshots | Implemented and validated: segment files, manifests, receipts, hashes and parent dependencies; compatible reuse does not require rebuilding |
| D06 | Qualification and explicit publication | Implemented and validated: functional, concurrent and recovery checks before publication; no fixed 24-hour wait |
| D07 | Version switching and dependencies | Active pointers, identity binding and retained history exist; rollback needs version checks, and parent segments must remain |
| D08 | Fresh-engine recovery | Internal roughly 501 GiB restore passed query acceptance; community native fixtures exercise the flow; not ClickHouse disaster recovery |
| D09 | All-discipline text indexes | Future scope entrypoint/build work; completed Earth candidate indexing does not imply every discipline is indexed |
| D10 | Vector/semantic indexes | Future: existing title/abstract retrieval is lexical and generates no embeddings |

### 2.5 HTTP API and retrieval tools

| ID | Capability | Status and boundary |
| --- | --- | --- |
| E01 | DOI/ID and entity detail | Implemented over imported metadata, independently of Earth text membership |
| E02 | Title, abstract and keyword text | Implemented within published index scope |
| E03 | Author-name work retrieval | Implemented in work author fields; distinct from author entity search |
| E04 | Year, OA, type and identity/source/topic filters | Supported fields follow capabilities and registries; not every raw JSON field has an efficient filter |
| E05 | Source/topic/author/institution names | Source/topic indexes exist; 0.3 adds recoverable, qualified institution and Earth-linked author catalogs; availability follows published build |
| E06 | Sorting, pagination, cursors and field selection | Implemented; official REST-style and local `/v1` routes coexist without full parity claims |
| E07 | Metadata grouping and sampling | Partial: `group_by`, `sample`/`seed` exist; arbitrary combinations with text search are not complete |
| E08 | Citation graph queries | Implemented: references, citations, related works and relevant counts; readable records with unresolved-target status |
| E09 | Bibliography matching | Partial: exact DOI and ranked title/author/year/source candidates; text scores are uncalibrated and cannot automatically confirm/merge papers |
| E10 | Concurrent and cross-Pod access | Supported and internally verified through reachable IP/port; default admission quotas/concurrency rejection disabled; resources still bound capacity |
| E11 | Complete official query semantics | Future incremental work: all fields/combinations/entity names, OQL, semantics and equivalent ranking are not delivered |
| E12 | Python SDK/MCP | Future: direct HTTP is available; standalone SDK/local MCP are extension candidates |

### 2.6 Web UI

| ID | Capability | Status and boundary |
| --- | --- | --- |
| F01 | Connect to the local API | Implemented: API key, service status and metadata/text-scope availability |
| F02 | Search and filters | Implemented: DOI/ID, indexed title/author-name text and year/OA/source/topic/type controls |
| F03 | Results and details | Implemented: readable title, DOI, authors/institutions, abstract, source, OA, labels and source JSON |
| F04 | Relationship browsing | Implemented: separate references, citing works and related works, pagination and record navigation |
| F05 | Save metadata and open OA links | Implemented: single-record JSON download and source-provided OA links; not bulk full-text downloading |
| F06 | Result exports, saved collections and download queues | 0.3 supplies query/list exports, selection, durable jobs and file downloads; saved collections/PDF queues remain future work |
| F07 | Version links and publication-first display | v0.4.0 optional arXiv source: unique exact DOI evidence, separate records, publication-first details and preprint switch; no fuzzy merges |
| F08 | Availability explanations | Basic status messaging exists; keep controls aligned with capabilities and distinguish unbuilt scope, missing source data and service errors |

### 2.7 CLI, deployment and operations

| ID | Capability | Status and boundary |
| --- | --- | --- |
| G01 | Source/workspace separation | Implemented: data, indexes, config, logs/reports stay outside Git; source snapshot can live independently |
| G02 | Native installers | Implemented: pinned versions/hashes, offline cache, default ClickHouse plus opt-in Manticore, Linux x86_64/ARM64 and explicit ARM compatibility profile |
| G03 | Plan, run, resume and status | Implemented: `pipeline plan/run/resume/status`, default metadata and explicit Earth profile |
| G04 | Lifecycle and diagnostics | Implemented: start/stop/restart, logs, status, healthz, readyz, capabilities; no automatic takeover of another host's live workspace |
| G05 | Existing-data adoption | Guarded read-only adoption exists; binding and qualification recorded separately; no automatic rewrite or backend replacement |
| G06 | Delivery and recovery | Internal full search recovery, cross-Pod and load evidence; native fixtures validate the portable workflow; no need to recreate validated indexes |
| G07 | Independent disaster backup | Further work: independent ClickHouse/config/source/index backups and restore exercises; search recovery alone is insufficient |
| G08 | Community stable release | Candidate freezes supported behavior, entrypoints and evidence; maintain the stable release from feedback, not by rebuilding existing pipelines |

## 3. Official repositories and extension candidates

These references identify extension opportunities. Except for explicitly delivered
features, they are not integrations. Reuse existing batch readers, task receipts,
queries and publication workflows instead of copying upstream production infrastructure.

| Extension | User result | Official reference | Integration boundary |
| --- | --- | --- | --- |
| X01 General result export (delivered in 0.3) | Save filtered works as IDs, DOI, JSONL and CSV | [openalex-api-tutorials](https://github.com/ourresearch/openalex-api-tutorials), [openalex-official](https://github.com/ourresearch/openalex-official) | Existing queries, batch reads and receipts; API/CLI/web; pinned generation/query/build and stable ID order |
| X02 Content downloads | PDFs and optional TEI XML from queries or DOI/ID lists | [openalex-official](https://github.com/ourresearch/openalex-official), [openalex-content-api](https://github.com/ourresearch/openalex-content-api) | Start with list export/official CLI handoff; actual access/formats follow source service policies |
| X03 Local OA downloads | Fetch accessible PDFs from snapshot locations | [openalex-official](https://github.com/ourresearch/openalex-official), [openalex-http](https://github.com/ourresearch/openalex-http) | Separate from official content service; record dead links/HTML/no-full-text; no universal PDF promise |
| X04 Content library/tasks | Link files to work IDs with retry, deduplication and failure lists | [openalex-official](https://github.com/ourresearch/openalex-official) | Keep content outside indexes; store type, hash, origin, time and status; credentials outside Git |
| X05 PDF/XML parsing | Extract abstract, authors/institutions and bibliography | [openalex-grobid](https://github.com/ourresearch/openalex-grobid), [parseland-lib](https://github.com/ourresearch/parseland-lib), [openalex-pdf-parser](https://github.com/ourresearch/openalex-pdf-parser) | Preserve separate extraction provenance/confidence; no raw-snapshot overwrite; parsing and matching are separate |
| X06 Cross-record versions (exact DOI subset delivered in 0.4) | Link preprint, published and repository records; prefer published display | [openalex-work-matcher](https://github.com/ourresearch/openalex-work-matcher) | Prefer explicit DOI/arXiv/version evidence, score text candidates and retain ambiguity; upstream code can require online APIs/models |
| X07 Better bibliography matching | Higher-quality candidates without DOI | [openalex-work-matcher](https://github.com/ourresearch/openalex-work-matcher) | Reuse text/matching APIs; add evaluation and calibrated decisions; existing scores do not establish identity |
| X08 Snapshot updates/deletions/merges | Fresh data, invalid-ID handling and redirected edges | [openalex-help](https://github.com/ourresearch/openalex-help), [openalex-walden snapshot](https://github.com/ourresearch/openalex-walden/tree/main/notebooks/snapshot) | Begin with reproducible new generations/cutover; update reverse citations/indexes as well as JSON |
| X09 Official query alignment | Broader filters, statistics, entities and query languages | [openalex-elastic-api](https://github.com/ourresearch/openalex-elastic-api), [openalex-help](https://github.com/ourresearch/openalex-help) | Borrow contracts/tests; retain ClickHouse+Manticore; upstream Elasticsearch code is not an immediately deployable local stack |
| X10 New text labels | Generate topics, keywords and SDGs for new records | [openalex-topic-classification](https://github.com/ourresearch/openalex-topic-classification), [openalex-keywords](https://github.com/ourresearch/openalex-keywords), [openalex-sdgs](https://github.com/ourresearch/openalex-sdgs) | Run only when needed; reading existing labels needs no model installation; record model versions |
| X11 AI-tool integration | MCP access to local search, records and citations | [openalex-mcp-server](https://github.com/ourresearch/openalex-mcp-server) | Adapt to local API/coverage; online semantics/quotas do not automatically apply |

### 3.1 A minimal complete content-download workflow

```text
Local search/filter or user DOI list
    → Freeze work ID/DOI/version/source list
    → Select official content service or accessible source OA locations
    → Retrieve PDF/TEI as supported by that source
    → Verify type/integrity and persist file, hash and status
    → Return manifest, file locations and failure reasons
    → Optional parsing → bibliography matching/version association
```

Three different operations use the word "download":

1. **Snapshot acquisition:** user-managed input acquisition; no project downloader.
2. **Metadata export:** delivered JSONL.gz/CSV/DOI/ID query/list output, with receipts and resume.
3. **Paper content retrieval:** future independent PDF/TEI functionality, not index construction.

Official CLI output can be one JSON/PDF/XML file per paper, unlike snapshot
manifest+JSONL shards. Ingesting that supplementary metadata needs a separate
adapter, deduplication and provenance; do not point the existing reader at it.

### 3.2 How the references inform this project

| Reference | Responsibility | Local design use |
| --- | --- | --- |
| [openalex-help](https://github.com/ourresearch/openalex-help) | Official data/access/query documentation | Source formats, terminology and compatibility targets |
| [openalex-api-tutorials](https://github.com/ourresearch/openalex-api-tutorials) | API analysis examples | User journeys and export/query examples |
| [openalex-elastic-api](https://github.com/ourresearch/openalex-elastic-api) | Elasticsearch-backed API endpoints | Query contracts/tests; no engine migration implied |
| [openalex-walden](https://github.com/ourresearch/openalex-walden) | Upstream data pipelines | Input/update semantics; no requirement to deploy its processing infrastructure |
| Content, matcher, classification and MCP projects above | Optional downstream capabilities | Separate, explicitly qualified integrations with their own dependencies/licenses |

## 4. Suggested development order

See [the next development plan](NEXT-STEPS.md) for milestones and acceptance conditions.

| Stage | Work | Work not required again |
| --- | --- | --- |
| Stable baseline | v0.3.2 API/web/CLI/exports delivered; maintain supported behavior | No technology reselection or reimplementation of validated import/build/publish/recovery |
| Source foundations | Provenance/identity/update contract, optional local arXiv input and version links | Preserve OpenAlex raw records; no fabricated W IDs or wholesale reimport |
| Entity access and freshness | Earth entity catalogs and links; arXiv OAI and OpenAlex release/API reconciliation | Name catalogs need not rebuild works; discovery does not establish complete synchronization |
| Wider retrieval scopes | Configurable domain/field/topic/ID selection; pilot useful AI/CS/engineering intersections | Reuse compatible Earth segments; no compulsory all-discipline vector store |
| Later content tools | Existing links, opt-in PDF/TEI jobs, then bibliography recovery where useful | No text reindex solely to download files |
| Ongoing maintenance | Deletions/merges, independent backups, quality/performance regressions | No need for the upstream Databricks/Airflow acquisition stack |

## 5. Further reading

- [Native deployment and commands](QUICKSTART.md)
- [Snapshot acquisition and formats](SNAPSHOT.md)
- [Query coverage](API-COVERAGE.md)
- [Validation scope](VALIDATION.md)
- [Official snapshot documentation](https://help.openalex.org/access/snapshot/)
- [Official content CLI](https://github.com/ourresearch/openalex-official)

Repository descriptions/code references were reviewed on 2026-11-10; the
purposes in section 3.2 were rechecked against public repository metadata on
2026-11-11. Upstream services, access rules and models can change: verify behavior
and licensing before integration. This inventory changes no production data and
does not bring external A/D graph writing into the core project.
