import pandas as pd
from pandas import DataFrame
from utils.tools import list_all_files_in_dir
from typing import List
from utils.config import profiles_conf
from controllers.postgres_contoller import PostgresController
from utils.secrets import postgres_secrets
from pathlib import Path


def read_csv(src_folder_path: Path, files: List[str], headers) -> DataFrame:
    temp_dfs = []
    for f in files:
        temp_df = pd.read_csv(f"{src_folder_path}/{f}", names=headers, engine="pyarrow")
        temp_dfs.append(temp_df)

    return pd.concat(temp_dfs)


def to_sql(df: DataFrame, client: PostgresController, dataset: str, table_schema: str = "public"):
    columns, rows = split_to_rows_columns(df)
    client.insert_execute_values(
        columns=columns,
        rows=rows,
        table_name=dataset,
        table_schema=table_schema
    )


def split_to_rows_columns(df: pd.DataFrame):
    return list(df.columns), [list(row) for row in df.values]


if __name__ == "__main__":
    conn = PostgresController(**postgres_secrets)
    dataset_name = "profiles"
    root_folder = Path(__file__).parent / "data"
    dataset_folder = root_folder / dataset_name
    files = list_all_files_in_dir(dataset_folder, ".gz")
    to_sql(
        read_csv(
            src_folder_path=dataset_folder,
            files=files,
            headers=profiles_conf["headers"],
        ),
        client=conn,
        dataset=dataset_name,
    )
