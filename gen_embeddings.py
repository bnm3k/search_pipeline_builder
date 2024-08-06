import os
import argparse
import pickle

import duckdb
import duckdb.typing as t
import pyarrow as pa
import polars as pl
import hnswlib

from fastembed import TextEmbedding
import numpy as np


def cli():
    project_root = os.path.dirname(__file__)
    default_db_path = os.path.join(project_root, "pg_weekly.db")
    default_index_path = os.path.join(project_root, "index.bin")

    parser = argparse.ArgumentParser(
        prog="gen_embeddings",
        description="generates embeddings for vector search",
    )

    parser.add_argument(
        "--db", help="path to db file", default=default_db_path, dest="db_path"
    )

    parser.add_argument(
        "--index",
        "-i",
        help="path to index file",
        default=default_index_path,
        dest="index_file_path",
    )

    args = parser.parse_args()
    return args


def main():
    args = cli()

    # check db path
    db_path = os.path.abspath(args.db_path)
    if not os.path.isfile(db_path):
        raise Exception(f"Invalid db path: '{db_path}'")

    # get model
    name = "BAAI/bge-small-en-v1.5"
    model = TextEmbedding(model_name=name, providers=["CUDAExecutionProvider"])
    model_description = model._get_model_description(name)
    dimension = model_description["dim"]

    print(f"Model {name} ready to use")

    with duckdb.connect(db_path) as conn:
        # function has to be registered before transaction begins
        def embed_fn(documents):
            embeddings = model.embed(documents.to_numpy())
            return pa.array(embeddings)

        conn.create_function(
            "embed",
            embed_fn,
            [t.VARCHAR],
            t.DuckDBPyType(list[float]),
            type="arrow",
        )
        print("Init function for embedding")

        # begin transaction
        conn.execute("begin")

        # WARNING: we have to build string rather than pass the dim as an
        # argument since DDL statements don't allow for parametrized queries
        conn.execute(
            f"""
        create or replace table embeddings(
            entry_id int unique not null,
            vec FLOAT[{dimension}] not null,

            foreign key(entry_id) references entries(id)
        );
        """,
        )

        conn.execute(
            """
            insert into embeddings by name
            (select
                id as entry_id,
                embed(title || '\n' || coalesce(content, '')) as vec
            from entries)
            """
        )
        print("Generate embeddings")

        # create index to speed up search

        # create vector similarity index
        # conn.load_extension("vss")
        # conn.execute("set hnsw_enable_experimental_persistence = true")

        # conn.execute(
        #     """
        #     create index entries_vec_index on embeddings
        #     using hnsw(vec)
        #     with (metric = 'cosine');
        # """
        # )
        # print("Create vector similarity index on embeddings")

        num_elements = conn.sql(
            "select count(*) as count from embeddings"
        ).fetchone()[0]

        data_tbl = conn.sql(
            "select entry_id as id, vec as vec from embeddings"
        ).fetchnumpy()

        index = hnswlib.Index(space="cosine", dim=dimension)
        index.init_index(max_elements=num_elements, ef_construction=200, M=48)

        ids = data_tbl["id"]
        ems = data_tbl["vec"].tolist()

        index.add_items(ems, ids)

        index.set_ef(50)  # ef should always be greater than k

        # overwrite index file
        index.save_index(args.index_file_path)

        print(f"Create Index at {args.index_file_path}")

        conn.execute("commit")


if __name__ == "__main__":
    main()
