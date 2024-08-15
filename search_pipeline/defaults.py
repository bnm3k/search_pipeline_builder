from pathlib import Path

_curr_file = Path(__file__)
_curr_dir: Path = _curr_file.parent

project_root: Path = _curr_dir.parent.absolute()
data_dir = project_root.joinpath("data")

raw_data_dir = data_dir.joinpath("raw_data")

db_path = data_dir.joinpath("pg_weekly.db")
schema_path = data_dir.joinpath("schema.sql")

index_dir = data_dir.joinpath("index")
default_model_name = "BAAI/bge-small-en-v1.5"
models_dir = data_dir.joinpath("models")
