from abc import ABC, abstractmethod
from enum import Enum
from re import search

import duckdb
import pyarrow as pa
from fastembed import TextEmbedding
import hnswlib

from . import defaults


class RankMetric(Enum):
    SCORE = 1
    DISTANCE = 2


class Searcher(ABC):
    rank_metric: RankMetric

    @abstractmethod
    def search(self, search_term, max_count):
        raise NotImplementedError

    def order_by(self):
        if self.rank_metric == RankMetric.SCORE:
            return "score", "desc"
        elif self.rank_metric == RankMetric.DISTANCE:
            return "distance", "asc"
        raise Exception(f"Invalid metric: {self.rank_metric}")


class NullSearcher(Searcher):
    # returns empty results
    def __init__(self):
        self.rank_metric = RankMetric.SCORE  # could be either
        return

    def search(self, search_term, max_count=None):
        schema = pa.schema(
            [
                pa.field("id", pa.uint64(), nullable=False),
                pa.field("score", pa.float64(), nullable=False),
            ]
        )

        return schema.empty_table()


class DuckDBFullTextSearcher(Searcher):
    def __init__(self, conn, ignore_max_count=True):
        # ignore max count on the basis that during retrieval, max count will
        # be applied in the limit clause
        self.ignore_max_count = ignore_max_count
        self.conn = conn
        self.rank_metric = RankMetric.SCORE

    def search(self, search_term, max_count):
        sql = f"""
        select id,
        fts_main_entries.match_bm25(id, $1) as score
        from entries
        where score is not null
        order by score desc
        """
        if self.ignore_max_count == False:
            sql += f"\n limit {max_count}"
        return self.conn.execute(sql, [search_term]).arrow()


class VectorSearcher(Searcher):
    def __init__(self, conn, model_name, use_index=False):
        model = TextEmbedding(
            model_name, cache_dir=defaults.models_dir, local_files_only=True
        )
        model_id, dimension, index_path = conn.execute(
            """
            select id, dimension, index_path
            from embeddings_metadata
            where model_name = $1
        """,
            [model_name],
        ).fetchone()
        index = None
        if use_index:
            index = hnswlib.Index(space="cosine", dim=dimension)
            index.set_ef(50)
            index.load_index(index_path)
            # hnswlib uses distance, the lower the better/closer
            rank_metric = RankMetric.DISTANCE
        else:
            # duckdb cosine uses similarity, the higher the better/closer
            rank_metric = RankMetric.SCORE

        self.conn = conn
        self.model = model
        self.model_id = model_id
        self.dimension = dimension
        self.index = index
        self.rank_metric = rank_metric

    def search(self, search_term, max_count):
        assert max_count is not None
        # embed query
        query_embedding = list(self.model.query_embed(search_term))[0]
        if self.index is not None:
            ids, distances = self.index.knn_query(query_embedding, k=max_count)
            return pa.Table.from_arrays(
                [pa.array(ids[0]), pa.array(distances[0])],
                names=["id", "distance"],
            )
        else:
            sql = f"""
            select
                id,
                array_cosine_similarity(vec, $1::FLOAT[{self.dimension}]) as score
            from embeddings_{self.model_id}
            order  by array_cosine_similarity(vec, $1::FLOAT[{self.dimension}]) desc
            limit {max_count}
            """
            return self.conn.execute(sql, [query_embedding]).arrow()


class RRF(Searcher):
    def __init__(self, conn, left: Searcher, right: Searcher, k=60):
        self.conn = conn
        self.left = left
        self.right = right
        self.rank_metric = RankMetric.SCORE
        self.k = 60

    def search(self, search_term, max_count):
        left_results = self.left.search(search_term, max_count)
        right_results = self.right.search(search_term, max_count)

        # add rankings to left side
        l_col, l_order = self.left.order_by()
        r_col, r_order = self.right.order_by()

        sql = f"""
        with l as (
            select id, rank() over(order by {l_col} {l_order}) as rank
            from left_results
        ), r as (
            select id, rank() over(order by {r_col} {r_order}) as rank
            from right_results
        )
        select
            id,
            coalesce(1.0 / ($1 + l.rank), 0.0) +
            coalesce(1.0 / ($1 + r.rank), 0.0) as score
        from l full outer join r using(id)
        order by score desc
        limit {max_count}
        """
        return self.conn.execute(sql, [self.k]).arrow()


def retrieve(conn, searcher: Searcher, search_term: str, max_count: int):
    search_results = searcher.search(search_term, max_count)
    col, order = searcher.order_by()
    return conn.sql(
        f"""
    select
        title,
        e.author,
        coalesce(e.content,'') as content,
        e.main_link,
        i.publish_date::varchar as date
    from entries e
    join issues i on e.issue_id = i.id
    join search_results r on e.id = r.id
    order by {col} {order}
    limit {max_count}
    """
    ).arrow()
