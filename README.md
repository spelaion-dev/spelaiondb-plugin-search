# Search engines for SpelaionDB

Elasticsearch, OpenSearch, Solr and Meilisearch, served through SpelaionDB's plugin protocol 1.1.
The core reads an HTTP console request (`METHOD /path`, then an optional JSON body); this plugin
sends that canonical request once and returns Arrow results. It never parses SQL or decides
what may run. Each connection uses its core-provided tunnel address, with TLS against the real
host, without environment proxies or redirects. Credentials arrive only in `connect`.

## Connections

The engine name is `elasticsearch`, `opensearch`, `solr` or `meilisearch`. The usual HTTP ports
are 9200, 9200, 8983 and 7700. A nonempty user selects HTTP Basic authentication; with no user,
the password is an Elasticsearch API key, or a Meilisearch bearer token. OpenSearch and Solr
use Basic authentication; token authentication for them is not implemented. TLS modes and custom certificate authorities follow SpelaionDB's contract.

The explorer lists indexes as collections under `indexes`. Solr currently lists standalone
cores; SolrCloud collection discovery is not implemented. Opening one runs a visible, bounded
request of at most 200 hits. Elasticsearch/OpenSearch hits keep `_index`, `_id`, `_score` and
source fields; nested values and aggregations remain JSON. The final `…` column preserves late
fields and values whose type differs from the first batch. Colliding source/metadata names
are prefixed until unique, so neither overwrites the other. Responses are bounded at 64 MiB.

This first version has no SQL endpoint, pagination beyond the submitted request, statement
cancellation, read-only mode, agents, relational edits, imports or streaming exports. The HTTP
connection is opened again when idle; a submitted request is never retried.

## Verification

Start `docker compose -p search-ci -f ci/compose.yaml up -d --wait`, then set:

```
SPELAION_TEST_ELASTICSEARCH=127.0.0.1:19200
SPELAION_TEST_OPENSEARCH=127.0.0.1:19201
SPELAION_TEST_SOLR=127.0.0.1:18983
SPELAION_TEST_MEILISEARCH=127.0.0.1:17700
```

`cargo test` runs each real engine through the plugin protocol, inserts a fixture, lists its
index and queries it, then runs the SDK's conformance set. Missing variables skip server tests;
a skipped test does not count as verification. Also run `cargo fmt --all --check`,
`cargo clippy --all-targets -- -D warnings` and `cargo deny check`.

In SpelaionDB's development build point all four `SPELAION_DEV_PLUGIN_<ENGINE>` variables at
this plugin executable and run `cargo test -p spelaion-core --test search` with the same servers.
Release builds accept only the published, verified build pinned in SpelaionDB's `plugins.toml`.

## Publication

SDB-183's first integration is prepared here. A release reaches users only after all four
platforms build in the shared release workflow and SpelaionDB pins those verified archives.
The core deliberately keeps the picker entries unavailable until that official pin exists;
a development build can exercise them through its explicit plugin overrides.
