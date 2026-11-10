# Architecture

The user downloads a snapshot using the official instructions. The toolchain
begins at a readable local snapshot directory; it never downloads or modifies
the source snapshot.

`Local snapshot → Read/verify → Metadata + citation import/audit → API/Web`

An optional branch adds text retrieval:

`Imported metadata → Example subset classification/collections → Text index → Search validation/recovery → Publication`

The default profile is `metadata` and requires ClickHouse. The supplied
`earth_sciences` profile is an opt-in example requiring Manticore as well. Its
rules, collections and index builders are separate modules, not requirements for
reading OpenAlex or serving metadata. Only this text-indexing example is supplied
in the current release; an arbitrary-discipline indexing framework is not claimed.

The source tree contains implementation, packaged rules, templates and synthetic
test fixtures. A workspace owns private configuration, ClickHouse data, immutable
search snapshots, checkpoints, exports, logs and reports. User-provided source
snapshots can live outside the workspace and are read in place.

Historical recovery modules and `baseline/` are compatibility code, not an
alternative default search stack. No external knowledge-graph project is required.
Only one writer may own a workspace; remote clients use HTTP. Two ClickHouse
processes must never open the same persistent data directory.

Optional catalog expansion reuses the published work segments, indexes imported institutions and linked authors, and derives identity-to-work candidates in ClickHouse. Queries and exports retain the original authorship predicate after candidate selection; large candidates use Manticore docid lookups through its existing HTTP SQL endpoint. Export workers own immutable query plans, shard receipts and durable output files independently of API processes. See [query/export operations](SEARCH-EXPORT.md).
