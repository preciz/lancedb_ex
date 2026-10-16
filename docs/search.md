# Full-text and hybrid search

[Back to README](../README.md#documentation).

These examples use `db` and `table` handles. See the [API guide](api.md) for setup.
Match the table schema to each example.

## Full-text and hybrid search

Create a full-text index on a string column, then search for words:

```elixir
{:ok, :created} = LanceDB.Index.create(table, :text, type: :fts, name: "text_fts")

{:ok, matches} =
  LanceDB.Query.full_text_search(table, "elixir concurrency",
    text_column: :text,
    columns: [:id, :text],
    filter: "category = 'code'",
    limit: 10
  )
```

Results include selected fields and `"_score"`, a BM25 relevance score.
Higher scores rank first. If no rows match, the result is `{:ok, []}`.
`:text_column` accepts a string or atom. Omit it to use the SDK's indexed
column selection. Set it explicitly when the table has multiple searchable
text columns.

`:columns` and `:filter` work as in ordinary queries. The default limit is
10 rows.

Indexed FTS uses the index's saved analyzer settings. The default analyzer
applies simple tokenization, lowercase conversion, English stemming,
stop-word removal, and ASCII folding. Unindexed scans use SDK defaults.
Plain strings are term queries. They are not SQL or a general query language. Use
`LanceDB.FTS` for structured queries, as described below. Set analyzer
options during index creation.

Text indexes support the same naming, replacement, inspection, deletion, and
maintenance operations as other indexes. An explicit `:text_column` also
permits unindexed scans. An FTS index speeds up repeated searches. Searches
can find new rows before index optimization.

Combine keyword matching and vector similarity with hybrid search:

```elixir
{:ok, matches} =
  LanceDB.Query.hybrid_search(table, [0.9, 0.1, 0.2], "elixir concurrency",
    vector_column: :vector,
    text_column: :text,
    columns: [:id, :text],
    distance: :cosine,
    filter: "category = 'code'",
    limit: 10,
    rrf_k: 60
  )
```

Hybrid search supports indexed and unindexed columns. Specify `:text_column`
for an unindexed text scan. It accepts all vector search options, including
`:nprobes`, `:refine_factor`, and `:bypass_vector_index`. It also accepts
`:text_column` and `:rrf_k`. Supply your own query embedding. A SQL filter
applies to both channels.

A row can match either channel. If one channel returns no matches, the other
channel can still return results.

By default, the SDK combines ranked candidates with **reciprocal rank fusion
(RRF)**. `:rrf_k` defaults to 60. It must be a positive number that float32
can represent. The SDK uses ranks that start at zero. Each channel supplies
`1 /
(rrf_k + rank)` for a candidate. Fusion adds these values.

Each channel fetches up to `limit + offset` candidates. After fusion, the
query skips the requested offset and returns at most `limit` results. Fusion
cannot find candidates outside those lists.

Results include selected data fields and `"_relevance_score"`. Higher
scores rank first. Results do not include individual channel scores.
`with_row_id: true` keeps internal row IDs. Without that option, results do
not include row IDs. The fused score combines ranks from both channels.

RRF scores indicate rank. They are not probabilities. Invalid options raise
`ArgumentError`. Native search failures return `{:error,
reason}`.

## Reusable text and hybrid queries

Use `Query.text/2` or `Query.hybrid/3` to select a source on a reusable query:

```elixir
alias LanceDB.{FTS, Query}

base =
  table
  |> Query.new()
  |> Query.where("category = 'code'")
  |> Query.select([:id, :text])

text_query =
  base
  |> Query.text(FTS.match("elixir concurrency", column: :text))
  |> Query.options(text_column: :text)
  |> Query.limit(10)

hybrid_query =
  base
  |> Query.hybrid([0.9, 0.1, 0.2], "elixir concurrency")
  |> Query.options(vector_column: :vector, text_column: :text, distance: :cosine, rrf_k: 60)
  |> Query.limit(10)
  |> Query.offset(0)
  |> Query.timeout(5_000)

{:ok, matches} = Query.all(text_query)
matches = Query.stream(hybrid_query, batch_size: 64)
{:ok, plan} = Query.explain(hybrid_query)
```

Both branches retain the base query's filter and selected columns. Builders
validate options without accessing storage. `Query.options/2` merges controls
for the selected source, including `:reranker` and `:normalize` for hybrid
queries. For computed projections, replace the base's `Query.select/2` call
with `Query.select_as/2`. Use `Query.order_by/2` for explicit ordering.
`Query.all/1`, row and batch streams, and diagnostics use the same query value.
See [query builders](queries.md#building-reusable-queries)
for all selectors and modifiers, including vector, batch, multivector, and direct
retrieval sources.

## Custom hybrid ranking and ordering

Hybrid search accepts `reranker: :rrf` by default. It also accepts a
two-argument Elixir function or a module that implements `LanceDB.Reranker`. A callback receives vector candidates and text candidates as lists of
maps. Both lists include `"_rowid"` and the requested columns or computed
projections. Vector rows have `"_distance"`. Text rows have `"_score"`.

`normalize: :score` normalizes each channel's scores directly. This is the
default. `normalize: :rank` first replaces scores with ranks, including tied
ranks. It then normalizes those ranks. Smaller distances stay better. Larger
text scores stay better.

This uses the current SDK's float32 normalization, including its handling of
almost constant scores. Candidate row order stays unchanged. RRF uses row
positions, so this option does not change RRF results.

For example, combine normalized vector similarity and text relevance:

```elixir
weighted = fn vector_rows, text_rows ->
  scores = Map.new(vector_rows, fn row ->
    {row["_rowid"], 0.7 * (1 - row["_distance"])}
  end)

  scores = Enum.reduce(text_rows, scores, fn row, scores ->
    contribution = 0.3 * row["_score"]
    Map.update(scores, row["_rowid"], contribution, &(&1 + contribution))
  end)

  scores
  |> Enum.map(fn {id, score} -> %{_rowid: id, _relevance_score: score} end)
  |> Enum.sort_by(&{-&1._relevance_score, &1._rowid})
end

{:ok, rows} = LanceDB.Query.hybrid_search(table, vector, "elixir",
  reranker: weighted,
  normalize: :score,
  limit: 20
)
```

Return scored row-ID maps directly or as `{:ok, maps}`. `{:error, reason}`
becomes a query error. IDs must come from either input channel. Each ID can
occur at most once in output. Scores must be finite numbers that float32 can
represent. The operation ignores additional map fields.

Original candidate data supplies result rows. A callback can remove
candidates or return an empty list. The result keeps callback order unless
you set `order_by`. Results include `_relevance_score`. Final results
include `_rowid` only with `with_row_id:
true`.

Callbacks run in a separate process. Callback failures become error tuples.
The query timeout includes candidate collection, callback execution, and
final processing. A timeout or caller termination cancels a running
callback. `:rrf_k` applies only to `:rrf`. It cannot be combined with a
custom reranker.

Plan and schema inspection do not call callbacks. Analysis executes the
callback and reports the final row count.

### Ordering and null placement

`order_by` works with scans, vector searches, full-text searches, hybrid
searches, and their streams. Vector searches include batch and multivector
searches:

```elixir
{:ok, rows} = LanceDB.Query.search(table, vector,
  order_by: [desc_nulls_last: :priority, asc: :id]
)

{:ok, rows} = LanceDB.Query.hybrid_search(table, vector, "elixir",
  order_by: [asc_nulls_first: :published_on, desc: :_relevance_score]
)
```

`:asc` and `:desc` keep the default nulls-last behavior. Use
`:asc_nulls_first`, `:asc_nulls_last`, `:desc_nulls_first`, or
`:desc_nulls_last` to set null placement for each key. Ordering uses stored
columns or query ranking metadata. This also applies when `columns` does not
include the sort column or a computed `select` alias uses its name. Use a
unique final key to resolve ties consistently.

Searches fetch up to `limit + offset` candidates before ordering and
pagination. Ordering cannot find rows outside that candidate set. Batch
searches apply this rule separately to each query vector. Hybrid channels
first keep their own relevance order. Fusion or the custom callback then
runs. Next, `order_by` sorts the combined results.

Finally, the query applies offset and limit. A different candidate count can
change rankings and page contents.

Hybrid streams collect both candidate sets before they return results. A
custom callback runs once when the caller reads the stream. A new stream
read calls it again. Final results stay in a native cursor. Elixir receives
batches with row counts limited by `batch_size`. Each channel still uses up
to `limit + offset` candidates.

Schema inspection and explain plans stay available. Invalid callback results
raise `LanceDB.Error` when the caller reads the stream.

## Structured full-text queries

Use `LanceDB.FTS` builders where the API accepts search text:

```elixir
alias LanceDB.FTS
query = FTS.match("elixir concurrency", column: :text, operator: :and)
{:ok, matches} = LanceDB.Query.full_text_search(table, query, columns: [:id, :text])
fuzzy = FTS.match("elixr", column: :text, fuzziness: 1, max_expansions: 50)
{:ok, matches} = LanceDB.Query.full_text_search(table, fuzzy)

query =
  FTS.boolean(
    must: [FTS.match("elixir", column: :text)],
    should: [FTS.match("concurrency", column: :title, boost: 2)],
    must_not: [FTS.match("deprecated", column: :text)]
  )

{:ok, matches} = LanceDB.Query.hybrid_search(table, [0.9, 0.1, 0.2], query)
{:ok, plan} = LanceDB.Query.explain(LanceDB.Query.new(table, {:text, query}, []), verbose: false)
```

Builders return `LanceDB.FTS` query values directly. Invalid arguments raise
`ArgumentError`. Database and index errors return `{:error, reason}` when
the query executes. Existing string queries keep their behavior. Structured
queries support projection, filters, limits, offsets, row IDs, fast search,
and timeouts.

A leaf's `:column` overrides the search call's default `:text_column`.
Without either option, automatic column selection works only if exactly one
text column has an FTS index. Otherwise, specify columns explicitly.
Multi-match always uses its named columns. Boolean clause lists accept
structured children and plain strings. Plain strings use exact OR term
queries.

| Builder | Behavior and options |
| --- | --- |
| `FTS.match(text, opts)` | `:operator` is `:or` by default or `:and`. Positive `:boost` defaults to 1. `:fuzziness` accepts 0, 1, or 2 and defaults to 0. `:max_expansions` defaults to 50. `:prefix_length` defaults to 0. |
| `FTS.phrase(text, opts)` | Matches ordered terms. Optional `:slop` must be non-negative and defaults to 0. Existing FTS indexes must store positions. An explicit column also permits unindexed scans. |
| `FTS.multi_match(text, columns, opts)` | Matches any named column. `:operator` combines terms within each column. `:boosts` supplies one positive weight for each column. All weights default to 1. |
| `FTS.boolean(opts)` | `:must`, `:should`, and `:must_not` are query lists. At least one must or should clause is required. With must clauses, should clauses supply optional scores. Without must clauses, at least one should clause must match. |
| `FTS.boost(positive, negative, opts)` | Returns positive matches. For rows that also match the negative query, it subtracts `negative_boost * negative_score`. The non-negative factor defaults to 0.5. Scores can become negative. This does not exclude rows. |

Phrase example:

```elixir
{:ok, :created} =
  LanceDB.Index.create(table, :text,
    type: :fts,
    name: "text_fts",
    replace: true,
    analyzer: [with_position: true]
  )

{:ok, matches} =
  LanceDB.Query.full_text_search(table, FTS.phrase("elixir concurrency", slop: 1),
    text_column: :text
  )
```

Rebuild an existing index with positions if necessary. Phrase queries also scan
unindexed current rows, including new appends, unless `fast_search: true` is used.
With no index, supply an explicit column to scan its text. Index optimization
makes appended rows searchable through indexed positions and can reduce scan work.

Fuzzy expansion uses the index vocabulary. It can miss terms that occur only
in unindexed data.

Fuzzy matching uses simple query tokenization in this SDK. This can differ
from the index's configured normalization. `:prefix_length` limits leading
Unicode code points. The package does not expose automatic fuzziness.

LanceDB 0.40.0 still treats that setting as exact matching at its public query
planning boundary. Set edit distances and
expansion limits explicitly.

## Preparing exact and fuzzy terms

`FTS.prepare/2` and `/3` analyze plain text once and return a reusable
`LanceDB.FTS.Prepared` value. Preparation uses either an explicit analyzer or
an index's saved settings. This is useful because exact queries use the index
analyzer, while positive fuzzy distances use basic tokenization: raw uppercase,
accented, or unstemmed text can fail fuzzy matches even when it matches exactly.

```elixir
alias LanceDB.{FTS, Query}

# Supply exactly one saved-analyzer selector: :column or :index.
{:ok, prepared} = FTS.prepare(table, "INTERSPRT NÜRNBRG", column: :text)

if FTS.term_count(prepared) > 0 do
  query =
    FTS.match(prepared,
      column: :text,
      operator: :and,
      fuzziness: {:length, 5, 9},
      exact_numbers: true,
      exact_boost: 2,
      max_expansions: 50
    )

  {:ok, rows} = Query.full_text_search(table, query)
end

# No database access is needed with explicit analyzer settings.
{:ok, prepared} =
  FTS.prepare("INTERSPRT NÜRNBRG",
    analyzer: [language: "German", remove_stop_words: false],
    max_bytes: 2048,
    max_terms: 32
  )

LanceDB.FTS.Prepared.terms(prepared)
# [%{analyzed: "intersprt", originals: ["INTERSPRT"]},
#  %{analyzed: "nurnbrg", originals: ["NÜRNBRG"]}]
```

For each distinct analyzed term, the builder combines an exact clause using
its first original spelling with a fuzzy clause using its analyzed form. With
AND, each term must match one of its own alternatives. With OR, any term may
match. A zero distance creates only the exact clause. The result is an ordinary
`FTS` query, usable in Boolean queries, hybrid search, and reusable query plans.
The same preparation can build either operator without further tokenization.

| Prepared match option | Behavior |
| --- | --- |
| `:fuzziness` | Defaults to 0. Accepts fixed 0, 1, or 2, or `{:length, one, two}` with `0 < one < two`. |
| `{:length, 5, 9}` | Analyzed terms below 5 Unicode graphemes stay exact; lengths 5–8 allow one edit; lengths 9 and above allow two. Length is measured after normalization, including stemming. |
| `:exact_numbers` | Defaults to true. Terms consisting entirely of Unicode decimal digits stay exact, including numbers beside punctuation. |
| `:exact_boost` | Defaults to 1. A positive multiplier applied to `:boost` for exact clauses. It expresses a scoring preference and does not guarantee all exact results rank first. |
| `:max_expansions` | Defaults to 50 and bounds vocabulary expansion separately for each fuzzy term. |
| `:prefix_length` | Defaults to 0 and counts leading Unicode code points of the analyzed fuzzy term. |

The other match options retain their usual meanings. Prepared-only options
are rejected for string-based `FTS.match/2` calls, whose behavior is unchanged.
Applications retain control over result filtering, ranking thresholds, and
fallback searches. Fuzzy expansion still depends on the indexed vocabulary
and may miss terms occurring only in unindexed rows. Exact alternatives can
match those rows.

Preparation supports plain-text documents with `tokenizer: :simple` and its
case, accent, stemming, stop-word, and token-length filters. It rejects code,
n-gram, raw, whitespace, and JSON analyzers. Unsupported explicit settings
raise `ArgumentError`; unsupported saved settings return `Error.kind == :unsupported`.
If normalization produces a term that basic fuzzy tokenization would split,
preparation also returns `:unsupported` rather than changing its meaning.

The native analyzer's byte offsets associate each retained term with its
original spelling. Stop-word and long-token removal omit terms. Repeated
analyzed terms become one query term while retaining their distinct original
spellings. They do not inflate scores. Empty, blank, punctuation-only, or fully
filtered text produces an empty preparation; matching it raises `ArgumentError`.

`:max_bytes` defaults to 2048 input bytes and `:max_terms` defaults to 32 distinct
retained analyzed terms. Limits are inclusive; nil disables either limit.
Invalid UTF-8, invalid options, and exceeded limits raise `ArgumentError`.
Native analyzer and storage failures return `LanceDB.Error`.

Saved preparation accepts the same index/column selectors as `Tokenizer.tokenize/3`,
including snapshots, and preserves missing, ambiguous, and non-FTS index errors.
It retains no table handle and does not bind the query column. Use it with an
index having the same settings. After changing an analyzer, prepare the text
again; query construction performs no storage access or settings validation.

## Token inspection

Inspect analyzer output without a table, or use an existing index's saved
settings:

```elixir
{:ok, tokens} = LanceDB.Tokenizer.tokenize("Running in cafés")
# [%{"text" => "run", "position" => 0}, %{"text" => "cafe", "position" => 2}]

{:ok, tokens} =
  LanceDB.Tokenizer.tokenize("MiXeD café",
    tokenizer: :raw,
    lower_case: false,
    stem: false,
    remove_stop_words: false,
    ascii_folding: false
  )

{:ok, tokens} = LanceDB.Tokenizer.tokenize(table, "Running in cafés", index: "text_fts")
{:ok, tokens} = LanceDB.Tokenizer.tokenize(table, "Running in cafés", column: :text)
```

`tokenize/2` accepts the same analyzer keyword list as index creation. For
`tokenize/3`, set exactly one selector: `:index` or `:column`. The
selector accepts a string or atom. Column selection works only if the column
has exactly one FTS index. Missing, ambiguous, and non-FTS indexes return
errors. Inspection reads saved metadata.

This includes the selected historical snapshot when the handle has a
historical checkout. Inspection does not change the table or index.

Results use string keys `"text"` and `"position"`. Positions start at zero.
Zero represents the first retained token even when leading terms are removed.
Stop-word removal can leave gaps between positions. Tokens show the
configured analyzer output. They do not show complete query execution or
fuzzy vocabulary expansion. Results do not include original character or
byte offsets.

The function accepts empty input. With the default analyzer, it returns
`{:ok, []}`. Invalid UTF-8 and invalid options raise `ArgumentError`.

## Configurable FTS analyzers

Configure tokenization and normalization when building an FTS index:

```elixir
{:ok, :created} =
  LanceDB.Index.create(table, :text,
    type: :fts,
    name: "text_fts",
    analyzer: [
      tokenizer: :simple,
      language: "English",
      lower_case: true,
      stem: true,
      remove_stop_words: false,
      ascii_folding: true
    ]
  )
```

`analyzer:` accepts a keyword list only for `type: :fts`. If you do not set
it, the SDK defaults apply. The index stores these settings. Keyword and
hybrid queries use them, even after you reopen the table.

| Option | Values and behavior |
| --- | --- |
| `:tokenizer` | Accepts `:simple`, `:whitespace`, `:raw`, `:ngram`, or `:code`. The default is `:simple`. |
| `:document_tokenizer` | Accepts `:text` by default or `:json` for JSON documents in a string column. |
| `:custom_stop_words` | Accepts a list of UTF-8 words. It replaces language stop words when `remove_stop_words: true`. `nil` uses language defaults. |
| `:split_identifiers`, `:split_on_numerics`, `:preserve_original`, `:index_operators` | Use these Boolean controls only with `tokenizer: :code`. |
| `:language` | Accepts an SDK language name, such as `"English"`, `"German"`, or `"French"`. The default is English. |
| `:lower_case` | Converts tokens to lowercase. |
| `:stem` | Applies stemming for the selected language. |
| `:remove_stop_words` | Removes stop words for the selected language. |
| `:ascii_folding` | Converts accented characters to ASCII equivalents. |
| `:with_position` | Stores token positions. This increases index size. |
| `:max_token_length` | Accepts a positive exclusive byte limit or `nil` to disable it. The default is 40; tokens with 40 or more bytes are removed before normalization. |
| `:ngram_min_length`, `:ngram_max_length` | Accept positive bounds. Both default to 3. The minimum cannot exceed the maximum. |
| `:ngram_prefix_only` | Generates only prefix n-grams. The default is false. |

Boolean options must be `true` or `false`. The simple analyzer defaults to
lowercase conversion, stemming, stop-word removal, and ASCII folding.
Positions default to false. The code tokenizer has separate SDK defaults.
N-gram analysis disables stemming and stop-word removal by default; explicit
options override these defaults.
For n-gram settings, use `tokenizer: :ngram`. N-grams cannot store
positions.

For SQL substring, `LIKE`, or regular-expression filters, see the separate
[n-gram scalar index](indexes.md#substring-and-pattern-indexes).

The package does not include tokenizers that use external language models.

For case-sensitive exact token matching, use `:raw`. Disable lowercase
conversion, stemming, stop-word removal, and ASCII folding. For n-gram
matching, consider disabling stemming and stop-word removal. Invalid option
structures raise `ArgumentError`. An unsupported language returns an SDK
error.

FTS index creation also accepts `block_size: 128 | 256`, positive
`memory_limit_mb`, positive `num_workers`, and
`format_version: :v1 | :v2 |
:v3`. These build settings are separate from
`analyzer:`. If no format is supplied, the SDK selects it. Use V3 for
blocks with 256 documents or for code tokenization. The SDK treats V3 as
experimental. Memory limits apply to index construction.

They do not limit total process memory.

For JSON document analysis, store valid JSON strings and construct scalar
matches with `FTS.json_match/3`:

```elixir
{:ok, :created} = LanceDB.Index.create(table, :text, type: :fts,
  analyzer: [document_tokenizer: :json])
query = LanceDB.FTS.json_match("metadata.language", "elixir", column: :text)
{:ok, rows} = LanceDB.Query.full_text_search(table, query, wand_factor: 1.0)
```

JSON queries accept strings, numbers, booleans, and `nil`. Dotted paths
select nested values. Arrays use their containing path. The current
tokenizer cannot escape commas or semicolons in paths. It also cannot escape
semicolons in values. The helper rejects these characters. Fuzzy matching
applies only to the scalar value; JSON paths and scalar types remain exact.

For token inspection with a JSON analyzer, use `path,type,value` syntax. For
example, use `title,str,elixir`.

Full-text and hybrid queries accept `wand_factor`. It must be `nil` or a
finite float32-compatible number greater than or equal to 1. A value of 1
keeps exact WAND pruning. Larger values permit faster top-k search with lower recall. Builders,
streams, and query diagnostics keep this setting. It does not apply to scans
or vector-only queries.

To change an analyzer, rebuild its index with the same name and
`replace: true`. Different options alone do not change an existing index.
Unindexed scans have no saved analyzer settings and use SDK defaults.
