from abc import ABC, abstractmethod
from enum import Enum

import pyarrow as pa


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
