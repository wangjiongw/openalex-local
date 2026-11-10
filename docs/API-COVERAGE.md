# Query coverage and limits

The default metadata profile serves source records and citation relationships.
Text indexing is optional; Earth science is the included example.

Use `/capabilities` and `/status` on your own running API for supported filters,
indexed entities, published scopes and generation. `/docs` is the executable route
reference. An official-style route does not imply full official API parity.

| Query | Local implementation | Coverage limit |
| --- | --- | --- |
| DOI / OpenAlex ID | complete source record lookup; DOI ambiguity explicit | imported metadata, independent of text indexes |
| Title / abstract / keywords | Manticore lexical full-text fields | published work index only; first profile is broad Earth science |
| Author names in works | author-name text search and work filters | works indexed in the published scope |
| Topic / source names | separate name indexes and ID filters | only entities included in that build; unrelated entity catalogs may remain unbuilt |
| Year, OA, source/topic/author IDs and combinations | indexed filters where supported; metadata routes for supported source predicates | inspect capabilities; arbitrary JSON predicates are not all indexed |
| References | outgoing snapshot `referenced_works` expanded to records | unresolved targets reported; empty source arrays are not inferred |
| Citations | reverse edges built from imported references | snapshot coverage and age limit completeness |
| Related works | snapshot `related_works` expanded to records | no new embedding or semantic-recommendation computation |
| Full record fields | preserved source JSON, plus selectable fields | missing source information remains missing |

An out-of-scope paper can still be resolved by DOI/ID and appear in references if
its metadata was imported, even if its title cannot be searched in the published
Earth index. Partial snapshots explicitly remain incomplete after publication.
Author-name search in papers is different from a separately built author entity
catalog. Preprints and published records retain their original IDs; automatic
cross-record version merging is not implemented in this release.

Official online ranking, live updates, every API filter/aggregation, every entity
name index and all-discipline text search are not promised. Parquet ingestion,
live deletion/merge application, arXiv integration and external A/D graph writing
are outside the community core release. Do not interpret a lexical text index as
an embedding index or missing references as zero true bibliographic references.

Development additions and export contracts are described in [SEARCH-EXPORT.md](SEARCH-EXPORT.md). Entity names become searchable only after their added catalog is qualified and published; identifier lookup retains full imported coverage.
