import os
import sys
import re
import argparse

import duckdb
import pyarrow as pa
from tantivy import SchemaBuilder, Index, Document

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from search_pipeline import defaults


def cli():
    parser = argparse.ArgumentParser(
        prog="build_tantivy_index",
        description="builds the tantivy full-text search index",
    )

    parser.add_argument(
        "--db", help="path to db file", default=defaults.db_path, dest="db_path"
    )

    parser.add_argument(
        "--index",
        help="path to tantivy index",
        default=defaults.tantivy_dir,
        dest="index_path",
    )

    args = parser.parse_args()
    return args


def main():
    args = cli()

    db_path = args.db_path
    index_path = args.index_path

    print(f"DB: {db_path}")
    print(f"Index path: {index_path}")

    schema = (
        SchemaBuilder()
        .add_integer_field("id", indexed=True, stored=True)
        .add_text_field("title")
        .add_text_field("author")
        .add_text_field("content")
        .add_text_field("tag")
        .build()
    )
    index = Index(schema=schema, path=str(index_path))
    writer = index.writer(heap_size=15_000_000, num_threads=1)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
        select
            id,
            title,
            coalesce(author,'') as author,
            coalesce(content,'') as content,
            coalesce(tag,'') as tag
        from entries
        order by id asc
        """
        )
        while True:
            row = conn.fetchone()
            if row is None:
                break
            id, title, author, content, tag = row
            doc = Document()
            doc.add_integer("id", id)
            doc.add_text("title", title)
            doc.add_text("author", author)
            doc.add_text("content", content)
            doc.add_text("tag", tag)
            writer.add_document(doc)
    writer.commit()
    index.reload()


if __name__ == "__main__":
    main()
