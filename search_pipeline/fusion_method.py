from abc import ABC, abstractmethod
from typing import Optional

from .common import SearchResult, RankMetric

import pyarrow as pa
import polars as pl


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

        concat_tbl = pa.concat_tables(
            with_std_schema(r.tbl) for r in search_results
        )

        # dedupe
        dedupe_tbl = (
            pl.from_arrow(concat_tbl)
            .unique(subset=["id"], keep="any")
            .to_arrow()
        )

        return SearchResult(dedupe_tbl, RankMetric.UNDEFINED, sorted=False)


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
