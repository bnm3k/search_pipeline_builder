import os
import argparse


def main():
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

    args = parser.parse_args()

    # check db path
    db_path = os.path.abspath(args.db)
    if not os.path.isfile(db_path):
        raise Exception(f"Invalid db path: '{db_path}'")
    print(f"DB path: '{db_path}'")


if __name__ == "__main__":
    main()
