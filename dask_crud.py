from utils.config import configuration
from utils.database_connector import DBConnector
from utils.secrets import secrets
from pathlib import Path
from dask.dataframe import DataFrame
from datetime import datetime
import dask.dataframe as dd
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename="log.txt", format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S")

path = Path(__file__).parent / "data"

uri = DBConnector(**secrets).get_conn_string("postgresql")


def read_csv(path: str, dataset: str) -> DataFrame:
    return dd.read_csv(
        f"{path}/{dataset}/*.gz",
        blocksize="128MB",
        names=configuration["headers"][dataset],
    )


def to_sql(ddf: DataFrame, tablename: str):
    start = datetime.now()
    ddf.to_sql(name=tablename, uri=uri, if_exists="replace", index=False)
    total_duration = datetime.now() - start
    logger.info(f"Dask job is finished. Total time: {total_duration.seconds} seconds")


if __name__ == "__main__":
    to_sql(read_csv(path, dataset="profiles"), "profiles")
    # to_sql(read_csv(path, dataset="activity"), "activity")
