# Small entity directories and work navigation

[中文](DIRECTORIES.zh-CN.md) · [API coverage](API-COVERAGE.md)

M2's first delivery adds metadata-backed name browsing for domains, fields, subfields, countries, continents, languages, licenses, institution/source/work types and SDGs. These are small imported source vocabularies, not new work text indexes. Empty imported vocabularies return empty results; no upstream values are fabricated.

`domain → field → subfield → topic` is the OpenAlex subject hierarchy. A domain is one of four broad groups; Earth and Planetary Sciences is a field within Physical Sciences. An arXiv category is a separate vocabulary. Choosing a hierarchy entry filters the already published OpenAlex work scope; it does not make all works in that subject text-searchable.

Both `/fields?search=Earth` and `/v1/fields?search=Earth` perform case-insensitive **literal display-name substring** matching, not the Boolean full-text grammar used for papers. Browse without a search term, use `filter=domain.id:domains/3` or `filter=field.id:fields/19` for source hierarchy constraints, and use `per_page=100&cursor=*` for signed, generation-pinned paging. Source records and `select` behavior are preserved. Available vocabularies and navigation fields are declared under `/capabilities.small_directories`.

The webpage provides domain/field/subfield selectors, country/license/language suggestions and directory-to-work navigation. Saved query URLs retain selected hierarchy IDs. Country filters refer to author-affiliation countries. The same work filters feed query exports:

```bash
curl -G "$BASE_URL/v1/fields" --data-urlencode 'search=Earth' --data-urlencode 'filter=domain.id:domains/3'
curl -G "$BASE_URL/v1/works" --data-urlencode 'filter=domain:domains/3,field:fields/19' --data-urlencode 'count=exact'
curl "$BASE_URL/v1/exports" -H 'Content-Type: application/json' -d '{"query":{"route":"/v1/works","params":{"filter":"domain:domains/3,field:fields/19"}},"formats":["jsonl.gz","csv"]}'
```

Add the deployment's authentication header when required. Metadata-only installations can browse directories without Manticore; work text search/navigation/export still requires the appropriate published work index. Ordinary directory browsing never starts builds, modifies source records or calls upstream services.

The reusable `directories` module declares small vocabularies and literal name predicates. Existing `official.execute` performs SQL filtering before counts/paging, `/v1` reuses that path, and existing query/export projections handle work filters. No new data-processing stage or works reindex is required. Parent fields absent in a source record remain absent.

Publisher/funder name catalogs and award-to-work expansion are the next M2 batch; they are not covered by small-vocabulary search. Existing author/institution/source/topic catalogs are reused. Live synchronization, Crossref and new domain work indexes remain independent later milestones.
