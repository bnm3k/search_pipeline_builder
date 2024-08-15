import sys
from abc import ABC, abstractmethod
from pathlib import Path
from enum import Enum
from typing import Callable, Optional


import pyarrow as pa
import polars as pl
import duckdb

from lib import defaults


class RankMetric(Enum):
    SCORE = 1
    DISTANCE = 2
    UNDEFINED = 3


class SearchResult:
    def __init__(self, tbl, rank_metric, *, sorted: bool):
        self.tbl = tbl
        self.rank_metric = rank_metric
        self.sorted = sorted

    def descending(self) -> bool:
        if self.rank_metric == RankMetric.SCORE:
            return True
        elif self.rank_metric == RankMetric.DISTANCE:
            return False
        raise Exception(f"Invalid metric: {self.rank_metric}")

    def col(self) -> str:
        if self.rank_metric == RankMetric.SCORE:
            return "score"
        elif self.rank_metric == RankMetric.DISTANCE:
            return "distance"
        elif self.rank_metric == RankMetric.UNDEFINED:
            return "undefined_metric"
        raise Exception(f"Invalid metric: {self.rank_metric}")

    def order_by_sql(self):
        if self.rank_metric == RankMetric.SCORE:
            return "score", "desc"
        elif self.rank_metric == RankMetric.DISTANCE:
            return "distance", "asc"
        raise Exception(f"Invalid metric: {self.rank_metric}")

    def retrieve(self, conn, max_count: int):
        search_results = self.tbl
        sql = f"""
        select
            title,
            e.author,
            coalesce(e.content,'') as content,
            e.main_link,
            i.publish_date::varchar as date
        from entries e
        join issues i on e.issue_id = i.id
        join search_results r on e.id = r.id
        """
        if not self.sorted:
            col, order = self.order_by_sql()
            sql += f"\norder by {col} {order}"
        if self.tbl.num_rows > max_count:
            sql += f"\nlimit {max_count}"

        return conn.sql(sql).arrow()


# ============================================================================
# BASE SEARCHERS
# ============================================================================
class BaseSearcher(ABC):
    @abstractmethod
    def search(self, query: str) -> SearchResult:
        ...


class NullSearcher(BaseSearcher):
    # returns empty results, for testing
    def __init__(self):
        pass

    def search(self, query):
        schema = pa.schema(
            [
                pa.field("id", pa.uint64(), nullable=False),
                pa.field("score", pa.float64(), nullable=False),
            ]
        )

        return SearchResult(
            schema.empty_table(), RankMetric.UNDEFINED, sorted=False
        )


class DuckDBFullTextSearcher(BaseSearcher):
    def __init__(self, conn, max_count: Optional[int] = None):
        self.conn = conn
        self.rank_metric = RankMetric.SCORE
        self.max_count = max_count

    def search(self, query: str) -> SearchResult:
        sql = f"""
        select id, fts_main_entries.match_bm25(id, $1) as score
        from entries
        where score is not null
        """
        if self.max_count is not None:
            sql += f"\n limit {self.max_count}"
        tbl = self.conn.execute(sql, [query]).arrow()
        return SearchResult(tbl, self.rank_metric, sorted=False)


class VectorSimilaritySearcher(BaseSearcher):
    def __init__(
        self,
        conn,
        *,
        model_name: str,
        max_count: int,
        index_dir: Path = defaults.index_dir,
        use_index: bool = False,
    ):
        from fastembed import TextEmbedding
        import hnswlib

        model = TextEmbedding(
            model_name,
            cache_dir=str(defaults.models_dir),
            local_files_only=True,
        )
        metadata = conn.execute(
            """
                select id, dimension, index_filename
                from embeddings_metadata
                where model_name = $1
            """,
            [model_name],
        ).fetchone()
        if metadata is None:
            raise Exception(
                f"Embeddings for model '{model_name}' does not exist"
            )
        model_id, dimension, index_filename = metadata
        index = None
        if use_index:
            assert (
                index_dir is not None
            ), "Must provide index dir path if use_index set to True"
            index = hnswlib.Index(space="cosine", dim=dimension)
            index.set_ef(50)
            index_path = str(index_dir.joinpath(index_filename))
            index.load_index(index_path)
            # hnswlib uses distance, the lower the better/closer
            rank_metric = RankMetric.DISTANCE
        else:
            # duckdb cosine uses similarity, the higher the better/closer
            rank_metric = RankMetric.SCORE

        self.conn = conn
        self.max_count = max_count
        self.model = model
        self.model_id = model_id
        self.dimension = dimension
        self.index = index
        self.rank_metric = rank_metric

    def search(self, query: str) -> SearchResult:
        # embed query
        query_embedding = list(self.model.query_embed(query))[0]
        if self.index is not None:
            ids, distances = self.index.knn_query(
                query_embedding, k=self.max_count
            )
            tbl = pa.Table.from_arrays(
                [pa.array(ids[0], type=pa.uint32()), pa.array(distances[0])],
                names=["id", "distance"],
            )
        else:
            sql = f"""
            select
                id,
                array_cosine_similarity(vec, $1::FLOAT[{self.dimension}]) as score
            from embeddings_{self.model_id}
            order by array_cosine_similarity(vec, $1::FLOAT[{self.dimension}]) desc
            limit {self.max_count}
            """
            tbl = self.conn.execute(sql, [query_embedding]).arrow()
        return SearchResult(tbl, self.rank_metric, sorted=True)


# ============================================================================
# FUSION METHODS
# ============================================================================
class FusionMethod(ABC):
    @abstractmethod
    def fuse(self, search_results: list[SearchResult]) -> SearchResult:
        ...


class ChainFusion(FusionMethod):
    def fuse(self, search_results: list[SearchResult]) -> SearchResult:
        std_schema = pa.schema(
            [pa.field("id", pa.uint32()), pa.field("score", pa.float64())]
        )

        # make all tables have same schema
        def with_std_schema(tbl):
            tbl = tbl.rename_columns(["id", "score"]).cast(
                target_schema=std_schema
            )
            return tbl

        tbl = pa.concat_tables(with_std_schema(r.tbl) for r in search_results)

        return SearchResult(tbl, RankMetric.UNDEFINED, sorted=False)


class ReciprocalRankFusion(FusionMethod):
    def __init__(self, k=60, max_count: Optional[int] = None):
        self.rank_metric = RankMetric.SCORE
        self.max_count = max_count
        self.k = 60

    def fuse(self, search_results: list[SearchResult]) -> SearchResult:
        df = (
            pl.concat(
                pl.from_arrow(r.tbl)
                .with_columns(
                    pl.col(r.col())
                    .rank(method="dense", descending=r.descending())
                    .alias("rank")
                )
                .drop(r.col())
                for r in search_results
            )
            .group_by("id")
            .agg(pl.col("rank").alias("ranks"))
            .with_columns(
                (
                    1
                    / pl.col("ranks")
                    .list.eval(pl.element() + self.k)
                    .list.sum()
                ).alias("score")
            )
            .drop("ranks")
        )
        return SearchResult(df.to_arrow(), RankMetric.SCORE, sorted=False)


# ============================================================================
# RERANKER
# ============================================================================


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


class MSMarcoCrossEncoder(Reranker):
    def __init__(self, conn):
        from sentence_transformers import CrossEncoder

        self.conn = conn
        self.encoder = CrossEncoder(
            "cross-encoder/ms-marco-MiniLM-L-6-v2", local_files_only=True
        )

    def rerank(self, query: str, search_result: SearchResult) -> SearchResult:
        docs = self.retrieve_docs(search_result.tbl)

        # rerank
        scores = self.encoder.predict(
            [(query, v) for v in docs["doc"].to_pylist()]
        )
        reranked_results = pa.Table.from_arrays(
            [docs["id"], scores], names=["id", "score"]
        )
        return SearchResult(reranked_results, RankMetric.SCORE, sorted=False)


# ============================================================================
def create_search_fn(
    base_searchers: BaseSearcher | list[BaseSearcher],
    *,
    fusion_method: Optional[FusionMethod] = None,
    reranker: Optional[Reranker] = None,
) -> Callable[[str], SearchResult]:
    if isinstance(base_searchers, BaseSearcher):
        base_searchers = [base_searchers]

    # add base searcher(s) and fusion method
    if len(base_searchers) == 0:
        raise Exception("Base_searchers should be > 0")
    elif len(base_searchers) == 1:
        assert (
            fusion_method is None
        ), "You provided a fusion method yet there is only 1 base searcher"
        first = base_searchers[0]
        searcher = lambda q: first.search(q)
    else:  # >= 2
        assert (
            fusion_method is not None
        ), "You must provide a fusion method if you provide >= 2 base searchers"
        searcher = lambda q: fusion_method.fuse(
            [s.search(q) for s in base_searchers]
        )

    # add reranker
    if reranker is not None:
        searcher_with_reranking = lambda q: reranker.rerank(q, searcher(q))
        return searcher_with_reranking
    else:
        return searcher


def main():
    query = "Logical Decoding and logical replication"
    max_count = 10
    with duckdb.connect(str(defaults.db_path), read_only=True) as conn:
        keyword_search = DuckDBFullTextSearcher(conn, max_count=None)
        semantic_search = VectorSimilaritySearcher(
            conn,
            model_name=defaults.default_model_name,
            max_count=10,
            use_index=True,
        )
        base_searchers = [keyword_search, semantic_search]
        fusion_method = ReciprocalRankFusion()
        reranker = MSMarcoCrossEncoder(conn)
        search = create_search_fn(
            [keyword_search, semantic_search],
            fusion_method=fusion_method,
            reranker=reranker,
        )
        got = search(query)
        search_results = got.retrieve(conn, max_count)
        conn.sql("select title from search_results").show()


if __name__ == "__main__":
    main()
