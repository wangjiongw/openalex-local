# Optional arXiv implementation contracts

Read [AGENTS.md](../../AGENTS.md) and [ARXIV.md](../ARXIV.md) first.

- `arxiv_source.py` is the standard JSONL/gzip and optional read-only DuckDB
  adapter. `normalize` preserves input, including unknown fields; a derived
  record must not claim original-JSON availability. Selection uses validated v1
  first submission; normalization requirements apply to selected records.
- `arxiv_pipeline.py` owns a separately named immutable release, full input hash,
  selector, processing signature and pinned OpenAlex proof. Existing generations
  and the qualified OpenAlex projection modules are untouched.
- SQLite is durable staging, not a public third search engine. DELETE journaling,
  FULL synchronous commits and one host/worker lock remain required. Cache size
  and read concurrency do not relax receipts or ownership.
- Linking substeps are ordered DOI evidence (`link-identifiers`), sorted unique
  OpenAlex work evidence (`link-works`), and exact record associations (`link`).
  Use existing `Backend.resolve_many/details`; offline clients use the ingestion
  profile so API reader execution limits do not abort long background batches.
  Thread workers read independently; only the owner commits ordered checkpoints.
- `LinkEvidence` implements only the two lookup operations used by `exact_links`.
  It preserves ambiguity and selected source identity fields, not full OpenAlex
  raw JSON. Cached and direct lookup must produce identical associations.
- `arxiv_index.py` owns its separate schema/projection; internal numeric doc IDs
  never become W IDs. MVA category filters require explicit `any(categories)`.
  Reuse the verified document writer, seal and restore, not the Earth membership
  orchestration. Signatures/receipts change through a new source release.
- `arxiv_api.py` keeps counts, cursors and exports source-specific. OpenAlex raw
  routes and bibliographies are unchanged. Publication preference requires exact
  evidence; ambiguity and missing DOI/reference fields remain explicit.
- `manticore_runtime.py` restores the optional source after existing OpenAlex
  recovery. Readiness can briefly be false during restore. Source failure must not
  terminate otherwise healthy OpenAlex serving. Native tests await readiness.
- Retain source staging, evidence, independent ClickHouse tables, index receipts,
  sealed files, active-pointer history and export-pinned releases. A software
  release or Git push does not publish data or upgrade a deployment.

Tests: `test_arxiv.py`; arXiv portion of `test_native_e2e.py` for indexing,
publication, filters, stable cursors, linked details, export and engine recovery;
`check_web_layout.py` for source switching, version preference and responsive UI.
Check optional-source absence/corruption and original OpenAlex behavior together.
