import pandas as pd
from pandas import DataFrame
from utils.tools import list_all_files_in_dir
from typing import List, Tuple
from utils.config import configuration
from utils.database_connector import DBConnector
from utils.secrets import secrets
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="log.txt", format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S"
)


def read_csv(src_folder_path: Path, files: List[str], headers) -> DataFrame:
    temp_dfs = []
    for f in files:
        temp_df = pd.read_csv(f"{src_folder_path}/{f}", names=headers, engine="pyarrow")
        temp_dfs.append(temp_df)

    return pd.concat(temp_dfs)


def to_sql(df: DataFrame, client: DBConnector, dataset: str):
    start = datetime.now()
    columns, rows = split_to_rows_columns(df)
    client.insert(
        columns=columns,
        rows=rows,
        table_name=dataset,
    )
    total_duration = datetime.now() - start
    logger.warning(
        f"Pandas job is finished. Total time: {total_duration.seconds} seconds"
    )


def split_to_rows_columns(df: pd.DataFrame):
    return list(df.columns), [list(row) for row in df.values]


if __name__ == "__main__":
    conn = DBConnector(**secrets)
    dataset_name = "profiles"
    root_folder = Path(__file__).parent / "data"
    dataset_folder = root_folder / dataset_name
    files = list_all_files_in_dir(dataset_folder, ".gz")

    to_sql(
        read_csv(
            src_folder_path=dataset_folder,
            files=files,
            headers=configuration["headers"][dataset_name],
        ),
        client=conn,
        dataset=dataset_name,
    )
