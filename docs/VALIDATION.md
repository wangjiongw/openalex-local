# Validation status

The community toolchain is version `0.2.0.dev0`. This records tested boundaries,
not an assertion of complete official API compatibility or full-snapshot capacity.

[Native CI for the toolchain implementation](https://github.com/wangjiongw/openalex-local/actions/workflows/tests.yml)
passed all four jobs on Linux x86_64 and ARM64: unit/build and native end-to-end.
The native jobs use pinned ClickHouse 25.8 LTS and Manticore 29.9.0. An additional
ARMv8 compatibility profile is exercised locally with the separately pinned
ClickHouse 26.9 runtime.

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
