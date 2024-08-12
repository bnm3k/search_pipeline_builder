import os
import argparse
import time

import duckdb
import pyarrow as pa
import hnswlib
import polars as pl
from fastembed import TextEmbedding

from great_tables import GT
from lib import defaults


def cli(search_strategies):
    parser = argparse.ArgumentParser(
        prog="search",
        description="carries out search on the pg weekly issues",
    )

    parser.add_argument(
        "--db", help="path to db file", default=defaults.db_path, dest="db_path"
    )

    parser.add_argument(
        "--index",
        "-i",
        help="path to index file",
        default=defaults.index_path,
        dest="index_file_path",
    )

    # use the index luke!
    parser.add_argument(
        "--use-index",
        help="Flag to turn on index scan (for vector-similarity based searches only)",
        action="store_const",
        const=True,
        default=False,
        dest="use_index",
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


def search_duckdb_vector_similarity(
    conn, search_term, max_count=20, use_index=False, index=None
):
    # get model
    name = "BAAI/bge-small-en-v1.5"
    model = TextEmbedding(model_name=name)
    model_description = model._get_model_description(name)
    dimension = model_description["dim"]

    # embed query
    query_embedding = list(model.query_embed(search_term))[0]

    results_df = None
    if use_index:
        assert index is not None
        ids, distances = index.knn_query(query_embedding, k=max_count)

        knn_tbl = pa.Table.from_arrays(
            [pa.array(ids[0]), pa.array(distances[0])],
            names=["entry_id", "distance"],
        )
        results_df = conn.sql(
            """
        select
            title,
            e.author,
            coalesce(e.content,'') as content,
            e.main_link,
            i.publish_date::varchar as date
        from entries e
        join knn_tbl k on e.id = k.entry_id
        join issues i on e.issue_id = i.id
        order by distance asc
            """
        ).pl()
    else:
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
    assert results_df is not None
    return results_df


def search_duckdb_hybrid(
    conn, search_term, max_count=20, use_index=False, index=None
):
    # get model
    name = "BAAI/bge-small-en-v1.5"
    model = TextEmbedding(model_name=name)
    model_description = model._get_model_description(name)
    dimension = model_description["dim"]

    # embed query
    query_embedding = list(model.query_embed(search_term))[0]

    # rrf parameter
    k = 60

    # use index?
    if use_index:
        ids, distances = index.knn_query(query_embedding, k=max_count)
        knn_tbl = pa.Table.from_arrays(
            [pa.array(ids[0]), pa.array(range(len(ids[0])))],
            names=["entry_id", "rank"],
        )
    else:
        knn_tbl = conn.execute(
            f"""
        select
            entry_id,
            rank() over(
                order by array_cosine_similarity(vec, $1::FLOAT[{dimension}]) desc
            ) as rank
        from embeddings
        """,
            [query_embedding],
        ).arrow()

    sql = f"""
    with lexical_search_results as (
        select
            id as entry_id,
            rank() over (
                order by fts_main_entries.match_bm25(id, $1) desc nulls last
            ) as rank
        from  entries
    ),
    semantic_search_results as (
        select entry_id, rank from knn_tbl
    ),
    rrf as (
        select
            coalesce(l.entry_id, s.entry_id) as entry_id,
            coalesce(1.0 / ($2 + s.rank), 0.0) +
            coalesce(1.0 / ($2 + l.rank), 0.0) as score
        from lexical_search_results l
        full outer join  semantic_search_results s using(entry_id)
        order by score desc
        limit {max_count}
    )
    select
        title,
        e.author,
        coalesce(e.content,'') as content,
        e.main_link,
        i.publish_date::varchar as date
    from entries e
    join rrf on e.id = rrf.entry_id
    join issues i on e.issue_id = i.id
    """
    results_df = conn.execute(sql, [search_term, k]).pl()
    return results_df


def get_index(file_path, dimension):
    index = hnswlib.Index(space="cosine", dim=dimension)
    index.set_ef(50)
    index.load_index(file_path)
    return index


def main():
    search_strategies = {
        "lexical": search_duckdb_fts,
        "semantic": search_duckdb_vector_similarity,
        "hybrid": search_duckdb_hybrid,
    }
    keys = list(search_strategies.keys())
    args = cli(keys)

    search_term = " ".join(args.search_terms)
    output_to_cli = args.output_to_cli

    # check db path
    db_path = os.path.abspath(args.db_path)
    if not os.path.isfile(db_path):
        raise Exception(f"Invalid db path: '{db_path}'")

    with duckdb.connect(db_path, read_only=True) as conn:
        search_strategy = args.search_strategy
        dimension = 384  # TODO: let's not hardcode this, maybe store in DB?
        index = None
        if args.use_index:
            print("Use index")
            index = get_index(args.index_file_path, dimension)
        start = time.time_ns()
        if search_strategy == "lexical":
            results_df = search_duckdb_fts(conn, search_term)
        elif search_strategy == "semantic":
            results_df = search_duckdb_vector_similarity(
                conn, search_term, use_index=args.use_index, index=index
            )
        elif search_strategy == "hybrid":
            results_df = search_duckdb_hybrid(
                conn, search_term, use_index=args.use_index, index=index
            )
        else:
            raise NotImplementedError(f"Search strategy: {search_strategy}")
        end = time.time_ns()
        duration_ms = (end - start) / 1000000

    num_results = results_df.select(pl.len())["len"][0]
    if num_results == 0:
        print(f"No results for '{search_term}'")
        return

    if output_to_cli:
        print(f"search took f{duration_ms} ms")
        print(results_df[["title", "author", "date", "content"]])
        return

    # else, output to webpage using Great tables
    num_results = results_df.select(pl.len())["len"][0]
    df = results_df
    output_to_great_tables(results_df, search_term, num_results, duration_ms)


if __name__ == "__main__":
    main()
