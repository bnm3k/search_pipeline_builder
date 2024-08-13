import os
from pathlib import Path

_curr_dir = Path(os.path.dirname(__file__))

project_root = _curr_dir.parent.absolute()

raw_data_dir = os.path.join(project_root, "raw_data")

db_dir = os.path.join(project_root, "db")
db_path = os.path.join(db_dir, "pg_weekly.db")
schema_path = os.path.join(db_dir, "schema.sql")
index_dir = os.path.join(db_dir, "index")
model_name = "BAAI/bge-small-en-v1.5"