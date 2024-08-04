import os
import argparse

import duckdb
import polars as pl
from fastembed import TextEmbedding

from great_tables import GT


def cli(search_strategies):
    project_root = os.path.dirname(__file__)
    db_path_default = os.path.join(project_root, "pg_weekly.db")

    parser = argparse.ArgumentParser(
        prog="search",
        description="carries out search on the pg weekly issues",
    )

    parser.add_argument(
        "--db", help="path to db file", default=db_path_default, dest="db_path"
    )

    parser.add_argument(
        "--strategy",
        "-s",
        help="search strategy to use",
        choices=search_strategies,
        default=search_strategies[0],
        dest="search_strategy",
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


def output_to_great_tables(df, search_term, num_results):
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
            subtitle=f"Search for '{search_term}', retrieved {num_results} results",
        )
        .fmt_markdown(columns="title")
        .fmt_date(columns="date", date_style="day_m_year")
        .cols_label(author="Author", date="Date", content="Overview")
        .cols_width({"title": "35%", "date": "120px"})
        .opt_stylize(style=2)
    )

    gt_tbl.show()


def search_duckdb_fts(conn, search_term, max_count=None):
    sql = f"""
    select
        title,
        e.author,
        coalesce(e.content,'') as content,
        e.main_link,
        i.publish_date::varchar as date
    from (
        select *,
        fts_main_entries.match_bm25(id, $1) as score
        from entries
    ) as e
    join issues i on e.issue_id = i.id
    where score is not null
    order by score desc
    """
    if max_count is not None:
        sql += f"\n limit {max_count}"
    results_df = conn.execute(sql, [search_term]).pl()
    return results_df


def search_duckdb_vector_similarity(conn, search_term, max_count=20):
    # get model
    name = "BAAI/bge-small-en-v1.5"
    model = TextEmbedding(model_name=name)
    model_description = model._get_model_description(name)
    dimension = model_description["dim"]

    # embed query
    query_embedding = list(model.query_embed(search_term))[0]

    sql = f"""
    select
        title,
        e.author,
        coalesce(e.content,'') as content,
        e.main_link,
        i.publish_date::varchar as date
    from entries e
    join embeddings em on e.id = em.entry_id
    join issues i on e.issue_id = i.id
    order  by array_cosine_similarity(vec, $1::FLOAT[{dimension}]) desc
    limit {max_count}
    """
    results_df = conn.execute(sql, [query_embedding]).pl()
    return results_df


def main():
    search_strategies = ["lexical", "semantic"]
    args = cli(search_strategies)
    search_term = " ".join(args.search_terms)
    output_to_cli = args.output_to_cli

    # check db path
    db_path = os.path.abspath(args.db_path)
    if not os.path.isfile(db_path):
        raise Exception(f"Invalid db path: '{db_path}'")

    with duckdb.connect(db_path, read_only=True) as conn:
        strategy = args.search_strategy
        if strategy == "lexical":
            results_df = search_duckdb_fts(conn, search_term)
        elif strategy == "semantic":
            results_df = search_duckdb_vector_similarity(conn, search_term)
        else:
            raise NotImplementedError(
                f"Search strategy: {args.search_strategy}"
            )

    num_results = results_df.select(pl.len())["len"][0]
    if num_results == 0:
        print(f"No results for '{search_term}'")
        return

    if output_to_cli:
        print(results_df[["title", "author", "date", "content"]])
        return

    # else, output to webpage using Great tables
    num_results = results_df.select(pl.len())["len"][0]
    df = results_df
    output_to_great_tables(results_df, search_term, num_results)


if __name__ == "__main__":
    main()
