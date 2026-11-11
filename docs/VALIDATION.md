# Validation status

[中文](VALIDATION.zh-CN.md) · [Documentation](INDEX.md)

The community toolchain is stable version `0.3.2`. This records tested boundaries,
not an assertion of complete official API compatibility or full-snapshot capacity.

## v0.3.2 UI and maintenance checks

The browser regression script uses synthetic API responses at widths 1440, 1024,
760 and 390. It checks layout, disclosures, relation direction/empty/error states,
source/API mismatch warnings, unresolved IDs, pagination, keyboard tabs, query URL
restoration, filter reset, active conditions, selection counts and service version.
The Linux x86_64 unit CI job runs this script in Chromium; native CI retains both
metadata/Earth profiles on x86_64 and ARM64.

An internal query-specific audit compared the first 20 results for `fengwu` against
all paginated reference targets; all sets matched stored source arrays. Two
preprint records also matched their exact decompressed source snapshot rows, both
with empty reference arrays. This is not a random corpus sample or an assertion
that all arXiv references are missing. Live UI checks exercise search, citations,
reference pagination, unresolved targets and export/downloads on existing indexes.
See the deployment's local reports for exact revisions and outcomes.

Only the version declaration and static UI assets change in the application from
v0.3.1. Query code, projections and engines are unchanged. Prior scale and recovery
evidence is reused with its original boundaries: no new full-corpus audit,
sustained performance qualification or independent ClickHouse disaster recovery
is implied. Public source releases and each installation's deployment are separate.

## v0.3.1 authentication policy checks

The patch adds an opt-in keyless deployment mode, preserving authenticated defaults.
Regression cases cover missing/wrong keys in both modes, OpenAPI security,
malformed configuration, retained body/deadline/rate guards and export routes.
The local deployment setting is separate from repository defaults. Query
projections, index layouts and engine versions remain unchanged; prior data and
search-recovery evidence is reused without claiming a new full-data rebuild.

## v0.3.0 stable promotion checks

Stable promotion changes only the application/package version declaration.
All 79 packaged program/resource files were compared with the qualified
v0.3.0-rc.1 wheel: only `__init__.py` differs, exactly by the version string.
The stable wheel, source and installed files match. Query code, projections,
engine versions, dependencies and UI resources are unchanged.

- Unit suite: 419 passed; 76 optional/live cases skipped.
- Source native workflows: both metadata-only and Earth profiles passed.
- Installed stable wheel, tested outside the source tree: both native profiles passed.
- The installed stable API passed 202 real-data functional checks on the existing
  published Earth index, without rebuilding or republishing it.
- Release CI covers unit/build and native checks on Linux x86_64 and ARM64;
  publication requires all four jobs to pass.

The candidate's sustained mixed-load and physical/search-recovery evidence below
is reused with this explicit equivalence boundary. It is not a new full-data
build, 30-minute performance run or independent ClickHouse recovery exercise.
The goal is stable use of supported local API behavior; broader official API
parity and new index scopes are not release gates.

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
