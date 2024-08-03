import os
import argparse

import duckdb
import polars as pl
from great_tables import GT


def cli():
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


def search(db_path, search_term):
    search_results_df = None

    with duckdb.connect(db_path, read_only=True) as conn:
        search_results_df = conn.execute(
            f"""
        select
            case
                when tag is not null
                    then coalesce(e.title,'') || ' (' || e.tag || ')'
                else coalesce(e.title,'')
            end as title,
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
                             """,
            [search_term],
        ).pl()
    assert search_results_df is not None
    return search_results_df


def main():
    args = cli()
    search_term = " ".join(args.search_terms)
    output_to_cli = args.output_to_cli

    # check db path
    db_path = os.path.abspath(args.db_path)
    if not os.path.isfile(db_path):
        raise Exception(f"Invalid db path: '{db_path}'")

    search_results_df = search(db_path, search_term)
    num_results = search_results_df.select(pl.len())["len"][0]
    if num_results == 0:
        print(f"No results for '{search_term}'")
        return

    if output_to_cli:
        print(search_results_df[["title", "author", "date", "content"]])
        return

    # else, output to webpage using Great tables

    num_results = search_results_df.select(pl.len())["len"][0]

    df = search_results_df
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


if __name__ == "__main__":
    main()
