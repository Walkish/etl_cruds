from utils.config import profiles_conf
from controllers.postgres_contoller import PostgresController
from utils.secrets import postgres_secrets
from pathlib import Path
from dask.dataframe import DataFrame
from datetime import datetime
import dask.dataframe as dd
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="log.txt", format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S"
)


def read_csv(path: Path, dataset: str) -> DataFrame:
    return dd.read_csv(
        f"{path}/{dataset}/*.gz",
        blocksize="128MB",
        names=profiles_conf["headers"],
    )


def to_sql(ddf: DataFrame, tablename: str, uri: str):
    start = datetime.now()
    ddf.to_sql(
        name=tablename, uri=uri, if_exists="replace", index=False, chunksize=50000
    )
    total_duration = datetime.now() - start
    logger.warning(
        f"Dask job is finished. Total time: {total_duration.seconds} seconds"
    )


if __name__ == "__main__":
    path = Path(__file__).parent / "data"

    uri = PostgresController(**postgres_secrets)._get_conn_string()
    to_sql(read_csv(path=path, dataset="profiles"), "profiles", uri=uri)
