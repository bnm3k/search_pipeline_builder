from enum import Enum
from typing import Optional


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
