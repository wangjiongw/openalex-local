# Changelog

## 0.2.0rc1 — v0.2.0-rc.1

- Handle dotted snapshot directories when importing persisted Manticore tables,
  using a temporary path alias without changing snapshot bytes or index layout.

- Removed `oa snapshot plan/download`; snapshot acquisition is now documented
  as a user-managed step. `snapshot verify` remains local-only.
- Default pipeline profile is now `metadata`; saved Earth plans must resume with
  `--profile earth_sciences`. `engines ensure --search` opts into Manticore.

- Community baseline extracted with a fresh public history.
- MIT licensing, contributor documentation, source/data separation policy.
- Separate installed code, packaged rules and local data workspaces.
- User-managed snapshot download guidance and local legacy/current JSONL adapters.
- Metadata-only default processing; Earth text indexing is an explicit optional example.
- Hash-pinned Linux amd64/arm64 native engines with offline cache support.
- Resumable metadata processing; optional Earth collections, indexing and search validation.
- Source-derived query/load checks and isolated fresh-engine search recovery.
- Guarded read-only adoption of explicitly compatible published installations.
- Native synthetic end-to-end CI; full production parity is not claimed.

Candidate scope is frozen. It preserves the existing metadata and search layouts;
internal `0.1.0` deployments are not automatically upgraded. Existing published
indexes can be reused only under the documented projection/version checks.
See [release and operations](docs/RELEASE.md) for supported commands and limits.
