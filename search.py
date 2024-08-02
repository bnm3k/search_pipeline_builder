import os
import argparse

import duckdb
from tabulate import tabulate


def cli():
    project_root = os.path.dirname(__file__)
    db_path_default = os.path.join(project_root, "pg_weekly.db")

    parser = argparse.ArgumentParser(
        prog="search",
        description="carries out search on the pg weekly issues",
    )
    parser.add_argument(
        "--db",
        help="path to db file",
        default=db_path_default,
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
    search_term = " ".join(args.search_terms)

    # check db path
    db_path = os.path.abspath(args.db)
    if not os.path.isfile(db_path):
        raise Exception(f"Invalid db path: '{db_path}'")

    with duckdb.connect(db_path, read_only=True) as conn:
        dim = os.get_terminal_size()
        width = dim.columns - 7
        title_width = width // 2
        content_width = width - title_width
        max_rows = dim.lines // 2 - 2
        res = conn.execute(
            """
        select
            coalesce(title,'')[:$3-3] as title,
            coalesce(content,'')[:$4-3] || '...' as content
        from (
            select *, fts_main_entries.match_bm25(
                id, $1
            ) as score
            from entries
        )
        where score is not null
        order by score desc
        limit $2
        """,
            [search_term, max_rows, title_width, content_width],
        ).fetchall()
        if len(res) == 0:
            print(f"No results found for: {search_term}")
            return

        output = tabulate(
            [r for r in res],
            headers=["Title", "Content"],
            tablefmt="simple_grid",
            maxcolwidths=[title_width, content_width],
        )
        print(output)


if __name__ == "__main__":
    main()
