from . import defaults
from .common import Searcher, RankMetric


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
