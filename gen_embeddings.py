import os
import argparse

import duckdb
import duckdb.typing as t
import pyarrow as pa
import polars as pl

from fastembed import TextEmbedding
import numpy as np


def cli():
    project_root = os.path.dirname(__file__)
    db_path_default = os.path.join(project_root, "pg_weekly.db")

    parser = argparse.ArgumentParser(
        prog="gen_embeddings",
        description="generates embeddings for vector search",
    )

    parser.add_argument(
        "--db", help="path to db file", default=db_path_default, dest="db_path"
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

        # create vector similarity index
        conn.load_extension("vss")
        conn.execute("set hnsw_enable_experimental_persistence = true")

        conn.execute(
            """
            create index entries_vec_index on embeddings
            using hnsw(vec)
            with (metric = 'cosine');
        """
        )
        conn.execute("commit")

        print("Create vector similarity index on embeddings")


if __name__ == "__main__":
    main()
