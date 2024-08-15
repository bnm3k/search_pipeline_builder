import os
import sys
import re
import argparse

import duckdb
import duckdb.typing as t
import pyarrow as pa
import hnswlib
from fastembed import TextEmbedding

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from search_pipeline import defaults


def cli():
    parser = argparse.ArgumentParser(
        prog="gen_embeddings",
        description="generates embeddings for vector search",
    )

    parser.add_argument(
        "--db", help="path to db file", default=defaults.db_path, dest="db_path"
    )

    parser.add_argument(
        "--index-dir",
        help="path to index dir",
        default=defaults.index_dir,
        dest="index_dir",
    )

    parser.add_argument(
        "--mode-name",
        help="name of text embedding model",
        default=defaults.default_model_name,
        dest="model_name",
    )

    args = parser.parse_args()
    return args


def main():
    args = cli()

    assert args.model_name is not None
    db_path = args.db_path
    model_name = args.model_name
    index_dir = args.index_dir

    print(f"DB: {db_path}")
    print(f"Model: {model_name}")

    with duckdb.connect(str(db_path)) as conn:
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
        conn.execute("begin")

        model = TextEmbedding(model_name, cache_dir=str(defaults.models_dir))
        model_description = model._get_model_description(model_name)
        dimension = model_description["dim"]
        norm_name = re.sub(r"[-\s\/]+", "-", model_name)
        index_filename = f"{norm_name}.bin"

        res = conn.execute(
            """
        insert into embeddings_metadata(model_name, dimension, index_filename)
        values ($1,$2,$3)
        returning id
        """,
            [model_name, dimension, index_filename],
        ).fetchone()

        model_id = res[0]

        conn.execute(
            f"""
        create or replace table embeddings_{model_id}(
            id uinteger unique not null references entries(id),
            vec FLOAT[{dimension}] not null
        );
        """,
        )

        conn.execute(
            f"""
            insert into embeddings_{model_id} by name
            (select
                id,
                embed(doc) as vec
            from documents)
            """
        )
        print(
            f"Generate and insert embeddings for model {model_id}: {model_name}"
        )

        # build index
        index_filepath = index_dir.joinpath(index_filename)
        num_elements = conn.sql(
            f"select count(*) as count from embeddings_{model_id}"
        ).fetchone()[0]

        data = conn.sql(
            f"select id, vec as vec from embeddings_{model_id}"
        ).fetchnumpy()

        index = hnswlib.Index(space="cosine", dim=dimension)
        index.init_index(max_elements=num_elements, ef_construction=200, M=48)

        ids = data["id"]
        ems = data["vec"].tolist()

        index.add_items(ems, ids)
        index.set_ef(50)  # ef should always be greater than k

        # overwrite index file
        index.save_index(str(index_filepath))
        print(f"Create index for model {model_id} at {index_filepath}")

        conn.execute("commit")


if __name__ == "__main__":
    main()
