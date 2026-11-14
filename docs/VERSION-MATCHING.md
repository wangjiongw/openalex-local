# Full-index version matching (v0.6 candidate)

[中文](VERSION-MATCHING.zh-CN.md) · [Version details](VERSIONS.md)

This optional workflow associates separate OpenAlex records. It reuses the
published works index and retains every confirmed group in its parent release,
including arXiv groups outside the selected discipline. It never overwrites work
JSON, references, OA, citation counts, search results, paging or export IDs.
The existing stable service can continue serving its published relationships
while the new workflow runs. Software deployment and relationship publication
are separate operations.

## Inputs and processing

Start with a verified metadata generation, published text build, verified
collection and published parent relationship release. The first implementation
requires that parent; the existing arXiv workflow can create it.

1. **Features:** sequentially read title, full author identities, year, type and
   identifiers from frozen Manticore attributes into a separate ClickHouse
   directory. No text reindex or per-record API call is required. Durable batch
   checkpoints and content digests support replay. Feature writes are read back;
   an interrupted checkpoint replays through idempotent replacing tables.
2. **Pilot:** rank only work IDs under an explicit 8 GiB/two-thread query bound,
   freeze the selected-ID receipt, then read back and checkpoint feature payloads
   in bounded batches. This avoids sorting complete payloads at full-corpus scale.
   Select approximately one million uniformly hash-ordered works
   across the entire published work index. This is independent of the old 100k
   pilot. Later batches exclude all previously processed IDs.
3. **Candidates:** normalized titles, author/content-word blocks and bounded
   native title-term fallback for preprints. Content-word blocks use the four
   longest informative words as a proxy for rare terms; corpus-frequency rare
   terms are not implemented. Shared DOI candidates resolve against all imported
   metadata and remain review candidates. Legacy explicit arXiv associations are
   inherited. Titles retrieve only from the published text scope.
4. **Review:** export disjoint study components for development (500 pairs),
   blind proposed novel associations (600) and difficult negatives (400). A small
   pilot may not supply these counts; that is an incomplete gate, not a pass.
5. **Groups and validation:** preserve the parent's complete snapshot and add
   only independently calibrated, directly verified publication stars. Every
   new member must match its publication anchor. Competing publications or links
   between existing groups remain candidates. Validate coverage and both
   directions of group membership before publication.

Features, candidate retrieval and confirmation policy have separate identities.
The feature directory includes its posting projection, so a change to that
projection requires another directory. Confirmation-only changes can reuse it.
Raw records are hydrated for proposed confirmations and explicit DOI targets
outside the text scope; candidate generation does not read all full work JSON.
Do not remove the feature database, checkpoints, retained parent releases or
index dependencies while a release references them.

## Conservative automatic association

The year window is ±10; missing years remain review. At most 200 candidates are
retained per work; saturation is marked `overflow_review`, never unmatched.
Author names are weak evidence. Require a shared ORCID or two stable author IDs;
both genuinely single-author records sharing one author ID are an exception.
Automatic bibliographic association is limited to preprint/publication pairs.
Notices, retractions, numerical/negation conflicts, duplicate DOI ambiguities,
publication duplicates and extensions remain review.

Title similarity is an explainable string score, **not a probability**. The
candidate policy exposes a 0.99/0.98/0.97/0.96 diagnostic grid; this initial
holdout freezes automatic acceptance at **0.99** and token overlap at **0.85**, with a minimum candidate margin of **0.03**.
Competing anchors are screened at the least strict grid value. Lower acceptance
thresholds require a new policy and an independent holdout, rather than tuning
against already-seen blind labels.

A human reviewer must supply `same_study`, `different_study` or `uncertain`, a
reviewer identifier and `independent_review: true`. Do not generate labels to
make the gate pass. All 600 blind pairs must be accepted true associations,
without automatic hard-negative errors. Zero errors in 600 gives a one-sided
95% precision lower bound of approximately 99.5%; this is not corpus recall.
Only strata with at least 100 blind examples are eligible. A failed or consumed
holdout cannot be relabeled to obtain a pass. Compatible later batches may reuse
a passed calibration with recorded parent provenance.

## Commands

Run the installed `oa` with the intended workspace. The examples use portable
release names; local paths and corpus counts do not belong in Git.

```bash
oa versions plan --profile full-index --release earth_match_pilot \
  --pilot-size 1000000 --workers 4
oa versions run --release earth_match_pilot --until review
oa versions status --release earth_match_pilot
# Stop safely, then continue the same frozen inputs:
oa versions stop --release earth_match_pilot
oa versions resume --release earth_match_pilot --until review
```

The release directory under `var/versions/` contains `review-development.json`,
`review-blind.json` and a private `review-package.json` with the frozen role map.
Reviewers use the first two; combine their labels into a JSON document containing
`identity`, `independent_review: true` and `pairs` (`id`, `label`, `reviewer`).
Do not expose the role map/scores to blind reviewers.

```bash
oa versions review-import --release earth_match_pilot --labels /local/reviews.json
oa versions run --release earth_match_pilot --until validate
oa versions publish --release earth_match_pilot
# Subsequent cumulative batches, core first, then author-supported, then broad:
oa versions plan --profile full-index --release earth_match_core \
  --stratum 01_core_journals --batch-size 5000000
```

Use frozen group names from the selected build. Run each new release through the
same stages. There is no unattended promotion past an incomplete audit. Without
labels, groups/validate can still create and publish a compatible snapshot of
inherited confirmed relationships plus new **pending** candidates; this never
confirms novel bibliographic pairs. The default `run` stops at review.

## API, web and operations

- `/v1/works/{id}/version-group`: confirmed group and original requested record;
  schema-2 responses also distinguish processed/not processed.
- `/v1/works/{id}/version-candidates`: evidence, review state, processing state,
  optional `relationship_release`, `per_page`, and a release/work-pinned cursor.
- `/v1/version-groups/{id}`: selected release's original member metadata.
- `/v1/works/{id}/versions` continues returning arXiv source associations.

The web page distinguishes confirmed members from pending candidates and shows
evidence without transferring bibliographies. Counts and exports remain records.
Feature coverage, matching coverage, audit and publication are separate states.
An entirely scanned collection does not mean every work has another version.

Progress files report committed records, candidate counts, overflows, processed
bytes, workers, speed and ETA. Start conservatively and compare query latency
under load before increasing workers. A same-Pod check does not establish remote
Pod reachability or high-concurrency capacity. Do not restart metadata/search
engines or bypass checksum validation for this workflow. Keep a compatible old
runtime and relationship pointer for rollback.
