# Validation status

[中文](VALIDATION.zh-CN.md) · [Documentation](INDEX.md)

The community toolchain is version `0.3.0rc1`. This records tested boundaries,
not an assertion of complete official API compatibility or full-snapshot capacity.

## v0.3.0-rc.1 retrieval and export checks

Application code frozen at `51b00dc`; subsequent release documentation edits do not change the qualified application. Public [four-job CI run](https://github.com/wangjiongw/openalex-local/actions/runs/42919290346) covers Linux x86_64/ARM64 unit/build and native workflows.

- Unit suite: 419 passed, 76 optional/live cases skipped. Both native profiles passed from source and from an installed wheel outside the source directory.
- Existing work coverage: 76,850,149 indexed Earth-example candidates. Work indexes and the full metadata generation were reused.
- Added institutions: 121,512. Earth-linked author membership: 28,816,816 IDs; 28,815,527 indexed; 1,289 referenced IDs absent from the imported author metadata and reported as missing. This is not the full all-discipline author-name catalog.
- All added segments passed snapshot hashes, native physical checks and independent restore checks. Parent full-index restore evidence was reused; no repeated parent restore or new ClickHouse disaster recovery is claimed.
- Real-data functional checks: 202 passed, including independent Earth bibliography and entity-name cases; application hashes stayed unchanged.
- Concurrent matrix: 1/4/8/16/32 clients for 60 seconds each, followed by 30 minutes of sustained queries alongside real export jobs. Sustained requests: 26,881; errors: 0; P95: 1.288 s; P99: 1.605 s. Export jobs completed: 2,180.
- At the 32-client stress point, matrix P95 was 3.798 s and P99 5.001 s, with 0 errors. Latency at this stress point is not a promise that every concurrency level stays below two seconds.
- A real 111,702-work institution query export passed cancellation/recovery across the worker update, with every unique output ID reconciled against the identity index and all downloaded shard hashes verified.
- Browser checks cover title/author/year, selection and CSV task creation, exact-job manifest download, query exports, saved URLs including invalid selectors, and desktop/mobile overflow. Local and real separate-Pod checks also exercise the installed package.

### Post-upgrade internal deployment checks

After deploying the same release package, actual-IP local checks and a real
separate Pod each passed 202 cases. The production API has four workers. A short
load check alongside other verification and internal traffic measured:

| Concurrent clients | Requests | P95 | P99 | Errors |
| --- | --- | --- | --- | --- |
| 8 | 349 | 0.784 s | 1.148 s | 0 |
| 32 | 202 | 8.757 s | 11.244 s | 0 |

This records capacity under the background load at that time, separately from
the single-process isolated qualification above. Zero errors at high concurrency
does not establish low latency; isolated measurements are not a promise for
other deployments.

These results describe this dataset and hardware. They are not a universal throughput promise, complete official API parity, or a measured classification recall guarantee. Workspace data, credentials, generated files and detailed internal acceptance reports remain outside Git.

## Previous v0.2.0-rc.1 candidate checks

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
