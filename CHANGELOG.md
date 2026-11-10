# Changelog

## Unreleased

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
