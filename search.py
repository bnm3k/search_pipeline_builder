import time
import argparse

import duckdb
import polars as pl
from great_tables import GT

from lib import defaults
import lib.search_methods as s


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
        "--searcher",
        "-s",
        help="search strategy to use",
        action="append",
        dest="searchers",
    )

    parser.add_argument(
        "--reranker",
        "-r",
        help="reranking method to use",
        dest="rerank_method",
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


class SearchBuilder:
    def __init__(self):
        self.complete_add_searchers = False
        self.l = None
        self.r = None
        self.reranker = None
        self.conn = None

    def set_conn(self, conn):
        self.conn = conn
        return self

    def add_searcher(self, searcher):
        assert (
            self.complete_add_searchers == False
        ), "Already done adding searchers"
        if self.l is None:
            self.l = searcher
            return self
        if self.r is None:
            assert self.l != searcher, "Cannot add same searcher twice"
            self.r = searcher
            self.complete_add_searchers = True
            return self
        raise Exception("Cannot add more than two searchers")

    def add_reranker(self, reranker):
        if reranker is not None:
            self.complete_add_searchers = True
            self.reranker = reranker
        return self

    def build(self):
        assert self.conn is not None, "Set duckdb conn"
        available_rerankers = {"rrf": lambda l, r: s.RRF(self.conn, l, r)}
        available_searchers = {
            "fts": lambda: s.DuckDBFullTextSearcher(self.conn),
            "vec": lambda: s.VectorSearcher(
                self.conn, model_name=defaults.model_name
            ),
        }

        assert self.l is not None, "Must include at least one searcher"
        left_searcher = available_searchers[self.l]()
        right_searcher = s.NullSearcher()
        if self.r is not None:
            right_searcher = available_searchers[self.r]()

        if self.reranker == "rrf":
            assert (
                self.l is not None and self.r is not None
            ), "RRF requires 2 search methods"

        if self.reranker is not None:
            assert (
                self.reranker in available_rerankers
            ), f"{self.reranker} not in {list(available_rerankers.keys())}"
            return available_rerankers[self.reranker](
                left_searcher, right_searcher
            )
        else:
            assert (
                self.r is None
            ), "You specified two search methods but no hybrid/reranking method"
            return left_searcher
        raise Exception("Invalid configuration for search builder")


def main():
    model_name = defaults.model_name
    db_path = defaults.db_path

    args = cli()

    search_term = " ".join(args.search_terms)
    output_to_cli = args.output_to_cli
    with duckdb.connect(db_path, read_only=True) as conn:
        b = SearchBuilder()
        for searcher in args.searchers:
            b.add_searcher(searcher)
        searcher = b.add_reranker(args.rerank_method).set_conn(conn).build()
        max_count = args.max_count

        start = time.time_ns()
        results_tbl = s.retrieve(conn, searcher, search_term, max_count)
        end = time.time_ns()
        duration_ms = (end - start) / 1000000

        results_df = pl.from_arrow(results_tbl)
        num_results = results_df.select(pl.len())["len"][0]
        if num_results == 0:
            print(f"No results for '{search_term}'")
            return

        if output_to_cli:
            print(f"search took {duration_ms} ms")
            print(results_df[["title", "author", "date", "content"]])
            return

        # else, output to webpage using Great tables
        output_to_great_tables(
            results_df, search_term, num_results, duration_ms
        )


if __name__ == "__main__":
    main()
