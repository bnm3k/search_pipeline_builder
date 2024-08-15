# Search Pipeline Builder

## Overview

Builder for combining keyword and semantic search, fusion methods and rerankers
to create a search pipeline. **[Arrow](https://arrow.apache.org/overview/)**
format is used for passing data between nodes in the pipeline.
[DuckDB](https://duckdb.org/) is used as the primary document store, metadata
store, full-text search index, vector store and also for ancillary tasks. A
pipeline consists of the following components:

- Base searchers
- Fusion methods
- Rerankers

The `create_search_fn` is then used to create a search function based on the
components provided. `create_search_fn` also carries validation, for example, it
does not make sense to set a fusion method if you've only got one base searcher.
`create_search_fn` has the following signature:

```python
def create_search_fn(
    base_searchers: BaseSearcher | list[BaseSearcher],
    *,
    fusion_method: Optional[FusionMethod] = None,
    reranker: Optional[Reranker] = None,
) -> Callable[[str], SearchResult]:
```

## Base Searchers: Carrying Out Keyword Search & Semantic Search

There are two categories of base searchers: keyword-based and semantic based.
All base searchers must inherit the following abstract class and implement the
required methods:

```python
class BaseSearcher(ABC):
    @abstractmethod
    def search(self, query: str) -> SearchResult:
        ...
```

For **keyword-based search**, also known as lexical search, DuckDB's
[FTS, Full Text Search Extension](https://duckdb.org/2021/01/25/full-text-search.html)
is used. Unless you are using a reranking method that gets slower the more
documents you rerank, it is recommended that you do not apply a limit/max-count
when using DuckDB's FTS. To create a DuckDB FTS base searcher, provide the
connection:

```python
conn = DuckDB(db_path)
base_searcher = DuckDBFullTextSearcher(conn)

query = "Who framed Roger Rabbit"
results = base_searcher.search(query)
```

For **semantic search**, [FastEmbed](https://qdrant.github.io/fastembed/) is
used for both generating and querying embeddings. All FastEmbed
[dense text embedding models](https://qdrant.github.io/fastembed/examples/Supported_Models/)
are supported out of the box. DuckDB is used to store the vectors and also the
model metadata (such as the dimensions). If you've got a few documents that you
are embedding, let's say 100s, then you probably do not need to add a vector
index to speed up similarity search - DuckDB's
[Array similarity](https://duckdb.org/docs/sql/functions/array.html) functions
will work just fine. However, with a large number of documents, you might have
to add an index for fast approximate search. For various reasons and possible
bugs that I have explored in
[my blog](https://bnm3k.github.io/blog/vss-duckdb-caveats), I do not use
DuckDB's built in vector index. Instead, I have opted for
[hnswlib](https://github.com/nmslib/hnswlib) for vector indexing. To create a
Vector Similarity base searcher, provide the connection (for fetching metadata
and non-index-based scan queries). Also, you must provide a `max_count`
especially for index-based/approximate scans which will not work without a
limit. Set `use_index` and provide the `index_dir`(where hnswlib will store and
read its index files):

```python
conn = DuckDB(db_path)
k = 20
base_searcher = VectorSimilaritySearcher(
    conn,
    model_name="snowflake/snowflake-arctic-embed-m",
    max_count=k,
    index_dir=dir,
    use_index=True,
)

query = "Logical Decoding in Postgres"
results = base_searcher.search(query)
```

## Fusing Results from Multiple Base Searchers

Fusion methods combine results from more than one base searchers. All fusion
methods must inherit from the following abstract class and implement the
requisite methods:

```python
class FusionMethod(ABC):
    @abstractmethod
    def fuse(self, search_results: list[SearchResult]) -> SearchResult:
        ...
```

The simplest fusion method is to concatenate the results. Hence **Chain Fusion**
which is modeled after the
[Rust's Chain Iterator](https://doc.rust-lang.org/beta/std/iter/struct.Chain.html).
Use this if you are going to apply reranking upstream and you do not care about
the scores assigned by base searchers. To rephrase it differently, since
separate base searchers assign separate score metrics, concatenating them
without normalizing or fusing the scores in some way results in a _garbage_
scores that should not be relied on. However, if you are going to apply
re-ranking, the the rerank method will calculate 'fresh' scores that you can
then use. This might be ideal in the case where keyword search might result in a
different set of results compared to semantic search and you want to include
both sets of results so as not to miss out on possible hits. Note that Chain
fusion also carries out deduplication in case a document appears from more than
one base searcher:

There is also **Reciprocal Rank Fusion** or RRF. RRF is described in this
[paper](https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf). It is a neat
and efficient way of combining and assigning scores to documents from different
results based on their respective ranks (rather than scores) within those result
sets. As such it can also be considered a reranking method. If you are using a
DuckDB FTS base searcher and did not set a limit for it, it is recommended that
you do so for RRF when using it, otherwise, you do not need to set `max_count`
for it. There is also the `k` parameter but it's best to leave it at 60, which
the authors recommend

```python
fusion_method = ReciprocalRankFusion()
search = create_search_fn(
    [keyword_search, semantic_search],
    fusion_method=fusion_method,
)

query = "Wrangling JSON with Postgres"
results = search(query)
```

## Reranking Results

The following rerankers are supported:

- [ragatouille's Colbert](https://ben.clavie.eu/ragatouille)
- [MS Marco Cross Encoder](https://www.sbert.net/docs/pretrained-models/ce-msmarco.html)
- [Jina AI's V2 reranker](https://huggingface.co/jinaai/jina-reranker-v2-base-multilingual)

Just in case you need to add some other reranker, all rerankers must inherit the
following class and implement the `rerank` method:

```python
class Reranker(ABC):
    conn: duckdb.DuckDBPyConnection

    @abstractmethod
    def rerank(self, query: str, search_result: SearchResult) -> SearchResult:
        ...

    def retrieve_docs(self, results: pa.Table) -> pa.Table:
        return self.conn.sql(
            """
        select id, d.doc
        from results r
        join documents d using(id)
        """
        ).arrow()
```

The `retrieve_docs` method is provided so that you can retrieve the actual
documents you've gotten from downstream searchers thus far so that you can
rerank them.

Please keep in mind Jina AI's license which limits you to non-commercial
use-cases.

Also, as often is the case with late-interaction models, you will probably need
to limit the number of documents you send in, to keep execution times
reasonable.

## End to end example

To put everything together, using the `create_search_fn` function, here's a
pipeline that takes the top 50 results based on keyword search and top 50
results based on semantic search, chains them together, then uses Colbert to
rerank them and retrieve the top 20 most relevant results:

```python
with duckdb.connect(str(db_path), read_only=True) as conn:
    # base searcher
    keyword_search = DuckDBFullTextSearcher(conn, max_count=50)
    semantic_search = VectorSimilaritySearcher(
        conn,
        model_name="BAAI/bge-small-en-v1.5",
        max_count=50,
        use_index=True,
    )

    base_searchers = [keyword_search, semantic_search]

    # get search fn
    search = create_search_fn(
        [keyword_search, semantic_search],
        fusion_method=ChainFusion(),
        reranker=ColbertReranker(conn, max_count=20),
    )
    # carry out search
    got = search(query)

    # get documents based on relevant doc IDs and score
    results_tbl = got.retrieve(conn, max_count)

    # display results
    conn.sql("select * from results_tbl").show()
```

## Contributing

Contributions are welcome. There is still a lot of stuff I need to figure out.
The most pressing matter is to make Search Pipeline Builder generic so that it
can be used in different domains.

Please fork the repository and create a pull request with your changes.

1. Fork the Project
2. Create your Feature Branch (git checkout -b feature/AmazingFeature)
3. Commit your Changes (git commit -m 'Add some AmazingFeature')
4. Push to the Branch (git push origin feature/AmazingFeature)
5. Open a Pull Request

## License

Distributed under the MIT License. See LICENSE for more information.

## TODO / Future Additions

- **Make more generic**: Or rather, make into a re-usable library. The project
  started of as a way of creating a full-text search over Postgres Weekly
  Newsletter listings (checkout this branch:
  [pg_weekly_search](https://github.com/bnm3k/search_pipeline_builder/tree/pg_weekly_search))
  so some parts of the code-base are still tightly bound to that use-case
- **Pre-filters** and **Post-filters**: add ability to filter documents e.g. on
  publish date or author before carrying out search and also after
- **Query Throughput**: use multi-processing or
  [ray.core](https://docs.ray.io/en/latest/ray-core/walkthrough.html) to support
  case where multiple queries can be ran in parallel. Currently, only
  intra-query parallelism is supported and queries are processed serially.
- **Sparse Embeddings Search**: Add support for searching sparse embeddings (as
  a base searcher) such as [Splade](https://github.com/qdrant/fastembed)
- **Fuzzy Search**: Add fuzzy search as one of the keyword searching methods.
- **Index Updating**: Add support for updating the index, e.g. when adding new
  documents or deleting old ones. Currently, one has to delete the indices and
  recreate it even though some of the underlying libraries used support in-place
  updates.
