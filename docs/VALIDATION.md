# Validation status

The community toolchain is version `0.2.0rc1`. This records tested boundaries,
not an assertion of complete official API compatibility or full-snapshot capacity.

## Candidate checks

Git candidate: `v0.2.0-rc.1`; package/API version: `0.2.0rc1`.
The runtime candidate is frozen at commit `4ed26c0`; subsequent release edits are
documentation-only. Public CI: [native and unit/build jobs](https://github.com/wangjiongw/openalex-local/actions/runs/42830328970).

| Check | Result and boundary |
| --- | --- |
| Unit suite | 374 passed, 76 explicitly skipped optional/live cases |
| Native CI | Linux x86_64 and ARM64 unit/build and metadata/Earth native workflows passed |
| Installed wheel | Outside-source ARMv8 compatibility run: both profiles passed, including dotted workspace paths |
| Existing-data API | 784 functional checks passed locally and 784 from an actual separate Pod; candidate code remained unchanged |
| Concurrent smoke | 60 seconds each at 1/8 concurrency: 165/639 requests, zero errors; P95 0.700/1.379 s, P99 0.902/1.910 s |
| Browser | DOI, title+author+year+OA, journal filtering, details, all three relation tabs, JSON download, desktop/mobile layout; no page errors |
| Existing deployment | Metadata/search pointers and generation/build manifests unchanged; no production cutover |

The short candidate load check is not a replacement for full production load
qualification. No fresh full-data reindex or large restore was performed for this
candidate. The internal baseline retains its previously completed roughly
501 GiB search restore, 784 recovery checks and 30-minute load evidence. This
candidate reuses that evidence without claiming it was rerun with new code.

The native jobs use pinned ClickHouse 25.8 LTS and Manticore 29.9.0. The local
ARMv8 compatibility run uses the separately pinned ClickHouse 26.9 runtime.

## Reproducible native checks

Native tests exercise both the default metadata-only profile (without installing
Manticore or creating Earth collections) and the optional Earth example.

The synthetic fixture contains 12 papers and six taxonomy/author/source records,
including Unicode, an unknown future field, missing references and an unresolved
reference target. Native checks cover metadata/source consistency, both reference
directions, Earth collection export, title and combination filters, fresh empty
search-engine recovery, explicit publication, authenticated HTTP access and
idempotent resume. This is deliberately a partial snapshot and short concurrent
load smoke, not a production throughput benchmark.

The installer also produces a wheel containing the web UI and rule/engine
resources; the same synthetic pipeline is exercised from an installed package
outside the source tree. Local snapshot verification checks gzip/JSON/counts. Snapshot acquisition is
user-managed and is not a project download feature.

Existing installations require separate checks: `adopt` binds only a supported
frozen projection; `verify` checks actual query fixtures and `restore` checks a
fresh search engine. The latter reuses metadata read-only and needs substantial
temporary disk for large datasets. A successful binding or a synthetic restore
does not certify full-data recovery. Physical Pod deletion and ClickHouse backup
recovery are not covered by this test suite.

Source-derived fixtures establish consistency; they do not measure independent
retrieval recall or classification precision. Cross-Pod checks require execution
from an actual remote client and are never inferred from a URL. Optional legacy
live-backend tests remain skipped unless explicitly opted in.
