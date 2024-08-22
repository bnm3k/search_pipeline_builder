import time
import argparse
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from enum import Enum
from typing import Callable, Optional


import pyarrow as pa
import polars as pl
import duckdb
from great_tables import GT

from search_pipeline import defaults
from search_pipeline import *


def output_to_great_tables(df, search_term, num_results, duration_ms):
    linkify = lambda s, l: pl.concat_str(
        [
            pl.lit("["),
            pl.col(s),
            pl.lit("]"),
            pl.lit("("),
            pl.col(l),
            pl.lit(")"),
        ]
    )
    df = df.with_columns(linkify("title", "main_link").alias("title"))
    gt_tbl = (
        GT(df[["title", "author", "date", "content"]], rowname_col="title")
        .tab_header(
            title="Postgres Weekly Search Results",
            subtitle=f"Search for '{search_term}', retrieved {num_results} results, took {duration_ms} ms",
        )
        .fmt_markdown(columns="title")
        .fmt_date(columns="date", date_style="day_m_year")
        .cols_label(author="Author", date="Date", content="Overview")
        .cols_width({"title": "35%", "date": "120px"})
        .opt_stylize(style=2)
    )

    gt_tbl.show()


def cli():
    parser = argparse.ArgumentParser(
        prog="search",
        description="carries out search on the pg weekly issues",
    )

    parser.add_argument(
        "--db", help="path to db file", default=defaults.db_path, dest="db_path"
    )

    parser.add_argument(
        "--limit",
        "-l",
        help="max count of results to return",
        default=20,
        dest="max_count",
    )

    parser.add_argument(
        "--cli",
        help="Flag to switch output to CLI instead of browser",
        action="store_const",
        const=True,
        default=False,
        dest="output_to_cli",
    )

    parser.add_argument(
        "search_terms",
        help="terms to search for",
        nargs="+",
        default=None,
    )

    args = parser.parse_args()
    return args


def main():
    args = cli()

    # config
    db_path = args.db_path
    query = " ".join(args.search_terms)
    output_to_cli = args.output_to_cli
    model_name = defaults.default_model_name
    max_count = args.max_count  # ignored for now

    with duckdb.connect(str(db_path), read_only=True) as conn:
        # base searcher
        keyword_search = TantivySearcher(max_count=50)
        semantic_search = VectorSimilaritySearcher(
            conn,
            model_name="BAAI/bge-small-en-v1.5",
            max_count=50,
            use_index=True,
        )

        base_searchers = [keyword_search, semantic_search]

        # fusion method
        # - others: ReciprocalRankFusion()
        fusion_method = ChainFusion()

        # reranker
        # -others: JinaRerankerV2(conn) ColbertReranker(conn, max_count=20)
        reranker = MSMarcoCrossEncoder(conn)

        # get search fn
        search = create_search_fn(
            [keyword_search, semantic_search],
            fusion_method=fusion_method,
            reranker=reranker,
        )

        # carry out search
        start = time.time_ns()
        got = search(query)
        end = time.time_ns()
        duration_ms = (end - start) / 1000000

        # get documents based on relevant doc IDs and score
        results_tbl = got.retrieve(conn, max_count)
        results_df = pl.from_arrow(results_tbl)
        num_results = results_df.select(pl.len())["len"][0]

        # output results
        if num_results == 0:
            print(f"No results for '{query}'")
            return

        if output_to_cli:
            print(f"search took {duration_ms} ms")
            print(results_df[["title", "author", "date", "content"]])
        else:
            # else, output to webpage using Great tables
            output_to_great_tables(results_df, query, num_results, duration_ms)


if __name__ == "__main__":
    main()
