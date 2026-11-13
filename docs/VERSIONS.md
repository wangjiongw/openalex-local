# OpenAlex version relationships

[中文](VERSIONS.zh-CN.md) · [API coverage](API-COVERAGE.md) · [Development plan](VERSION-GROUPS-PLAN.md)

The optional version workflow associates separate OpenAlex records using the
published arXiv source's verified exact DOI evidence. It preserves every original
W ID, field and reference list. It does not change search counts or pagination.
Software installation, relationship publication and API upgrades are separate steps.

## Inputs and reusable stages

Start with a qualified metadata generation, a published [arXiv source](ARXIV.md)
and a completely verified Earth collection with published Manticore receipts.
The first workflow intentionally uses Earth as a bounded pilot. It needs no new
work text index and downloads no external data during queries.

```bash
oa --workspace /path/to/workspace versions plan --release earth_versions_v1
oa --workspace /path/to/workspace versions run --release earth_versions_v1
oa --workspace /path/to/workspace versions status --release earth_versions_v1
oa --workspace /path/to/workspace versions validate --release earth_versions_v1
oa --workspace /path/to/workspace versions publish --release earth_versions_v1
```

`--generation`, `--arxiv-release`, `--collection` and `--seed` select the plan's
immutable inputs. The default seed is `20261113`. A changed input or processing
signature requires a new relationship release. `run --until <stage>` supports
staged operation. Stages are separately checkpointed:

| Stage | Processing and durable output |
| --- | --- |
| `seeds` | Replay **every** selected arXiv record's exact links against its frozen cached identifier/target evidence; retain multi-member and ambiguous decisions |
| `sample` | Deterministically sample 30,000 core-journal, 35,000 broad/interdisciplinary and 35,000 author-supported works from verified receipts; save IDs, strata, source hashes and sample hash |
| `targets` | Resolve all confirmed seed targets and pilot records against the pinned metadata; compare source identity fields and pilot raw hashes |
| `pilot` | Retrieve duplicate-DOI and explicit arXiv-link candidates; reuse title/author/year candidate retrieval for sampled preprints without a duplicate-DOI candidate |
| `groups` | Confirm only conflict-free preprint/publication stars supported by existing exact evidence; retain review/conflict decisions |
| `store` | Write separate ClickHouse groups, members and review tables with replay-safe write reconciliation |
| `validate` | Recheck frozen inputs, full seed/target completion, sample identity, all relationship rows and membership before publication |

`stop` requests cancellation at the next safe batch boundary; `resume` clears the
request and reuses committed batches. Terminal errors remain visible in `status`.
Do not run concurrent writers for one workspace. The initial sample is uniform
within each frozen stratum, not representative of all OpenAlex or a labeled
benchmark for semantic version matching. Only the sampled records receive new
pilot evidence processing; existing arXiv seeds span all source disciplines.

## Conservative confirmation

A confirmed source needs at least two distinct W IDs and one unambiguous
publication. A preprint-only match never invents a publication. Stars may share
a publication; conflicting publication choices block the affected stars instead
of confirming a transitive chain. Retractions and correction/erratum notices are
review relationships, never automatic preferred publications.

Repeated DOIs, explicit source links and title/author similarities remain
candidates unless the existing exact evidence independently confirms the pair.
Different titles can be versions; identical titles can be different papers.
No fuzzy confirmation threshold or exhaustive full-Earth matching is claimed.

## API and web

- `GET /v1/works/W123/version-group` returns the requested original record,
  relationship release, group members, per-member full records, evidence and
  `preferred_openalex_id`. A valid ungrouped record returns `state: unmatched`.
- `GET /v1/version-groups/vg_<hash>` returns the group JSON for the **currently
  published** relationship release. Local group IDs are release-specific.
- Existing `GET /v1/works/W123/versions` continues to return **arXiv source
  records**. It is a separate contract.
- `/status.version_groups` and `/capabilities.version_groups` report optional
  readiness, seed completion, pilot coverage and counts. An unpublished/broken
  optional module does not make ordinary OpenAlex lookup/search unavailable.
  Group endpoints return 503 while unavailable; missing works/groups return 404.

Web details prefer a unique confirmed publication and label both the matched
and displayed record. Manual member selection remains selected. Evidence is
inspectable and the group JSON can be downloaded. References, authors, abstracts,
OA, DOI, licenses and dates always come from the displayed member. Citation counts
are never added across versions. Existing selected/query exports retain original
record semantics; background grouped export and grouped search are deferred.

## Storage and rollback

Workspace state lives in `var/versions/<release>/`: immutable plan, durable
`staging.sqlite`, `sample.json`, validation and `lineage.json`. ClickHouse uses
`oa_versions_<release>.groups`, `.members`, `.reviews`; neither existing raw
metadata nor reference edges are rewritten. Retain these files, tables and their
pinned arXiv/source/search dependencies outside Git.

`var/versions-active.json` includes the prior pointer. To switch back, revalidate
and publish the retained prior release with its compatible application; group
membership is reconciled before changing the pointer. Lineage records previous
group IDs and their shared-member successors, including retirement/splits. It is
not an upstream OpenAlex merge map. Keep the prior application environment as
well; software rollback and relationship rollback are independent.

A one-record source association also respects `automatic_display_allowed`: notices and retracted records are not selected automatically.

[Full-index matching candidate](VERSION-MATCHING.md)
