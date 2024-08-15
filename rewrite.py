import sys
from abc import ABC, abstractmethod
from pathlib import Path
from enum import Enum
from typing import Callable, Optional


import pyarrow as pa
import polars as pl
import duckdb

from search_pipeline import defaults
from search_pipeline import *


def main():
    query = "Logical Decoding and logical replication"
    max_count = 20
    with duckdb.connect(str(defaults.db_path), read_only=True) as conn:
        keyword_search = DuckDBFullTextSearcher(conn, max_count=None)
        semantic_search = VectorSimilaritySearcher(
            conn,
            model_name=defaults.default_model_name,
            max_count=max_count,
            use_index=True,
        )
        base_searchers = [keyword_search, semantic_search]
        # fusion_method = ReciprocalRankFusion()
        fusion_method = ChainFusion()
        reranker = MSMarcoCrossEncoder(conn)
        # reranker = JinaRerankerV2(conn)
        # reranker = ColbertReranker(conn, max_count=10)
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
