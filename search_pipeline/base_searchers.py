from abc import ABC, abstractmethod
from typing import Optional
from pathlib import Path

import pyarrow as pa

from .common import SearchResult, RankMetric
from .defaults import index_dir, models_dir


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
        index_dir: Path = index_dir,
        use_index: bool = False,
    ):
        from fastembed import TextEmbedding
        import hnswlib

        model = TextEmbedding(
            model_name,
            cache_dir=str(models_dir),
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
