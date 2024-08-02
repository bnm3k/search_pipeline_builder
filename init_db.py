import os
import argparse

import duckdb

project_root = os.path.dirname(__file__)
db_path_default = os.path.join(project_root, "pg_weekly.db")
schema_path_default = os.path.join(project_root, "schema.sql")


def init_db(db_path, schema_path=schema_path_default):
    with duckdb.connect(db_path) as conn, open(schema_path, "r") as f:
        sql = f.read()
        got = conn.sql(sql)


def main():
    parser = argparse.ArgumentParser(
        prog="init_db",
        description="creates and sets up the db where the parsed data and FTS will be stored",
    )
    parser.add_argument(
        "--db",
        help="path to where db will be written to",
        default=db_path_default,
    )
    parser.add_argument(
        "--schema",
        help="path to schema file for init db",
        default=schema_path_default,
    )
    args = parser.parse_args()

    # check db path
    db_path = os.path.abspath(args.db)
    if os.path.exists(db_path):
        raise Exception(f"Invalid db path (already exists): '{db_path}'")
    print(f"DB path: '{db_path}'")

    # check schema path
    schema_path = os.path.abspath(args.schema)
    if not os.path.isfile(schema_path):
        raise Exception(f"Invalid schema path: '{schema_path}'")
    print(f"Schema path: '{schema_path}'")

    init_db(db_path, schema_path)
    print("Init DB: OK")


if __name__ == "__main__":
    main()
