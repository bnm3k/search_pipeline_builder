import os
from pathlib import Path

_curr_dir = Path(os.path.dirname(__file__))

project_root = _curr_dir.parent.absolute()

data_dir = os.path.join(project_root, "data")

raw_data_dir = os.path.join(data_dir, "raw_data")

db_path = os.path.join(data_dir, "pg_weekly.db")
schema_path = os.path.join(data_dir, "schema.sql")

index_dir = os.path.join(data_dir, "index")
model_name = "BAAI/bge-small-en-v1.5"
models_dir = os.path.join(data_dir, "models")
