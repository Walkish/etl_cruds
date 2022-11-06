import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Union, Tuple
from controllers.db_controller import DatabaseController
from datetime import datetime
import logging
from pandas.io.sql import get_schema

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="log.txt", format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S"
)


class PostgresController(DatabaseController):
    def __init__(
            self,
            host: Union[str, int],
            port: int,
            user: str,
            password: str,
            database: str,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.is_reconnect = False
        self.reconnect_number = 0

    def _get_conn_string(self, vendor: str = "postgresql"):
        return f"{vendor}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def _connect(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def disconnect(self):
        if not self._connection_is_closed():
            self.conn.close()

    def _connection_is_closed(self) -> bool:
        if self.conn is None or self.conn.closed != 0:
            return True
        else:
            return False

    def _check_and_reconnect(self):
        if self._connection_is_closed():
            if self.is_reconnect:
                self.reconnect_number += 1
            else:
                self.is_reconnect = True
            self._connect()

    def select(self, query: str) -> List[List]:
        rows, _ = self.select_with_columns(query)
        return rows

    def select_with_columns(self, query) -> Tuple[List[List], List[str]]:
        self._check_and_reconnect()
        with self.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
            rows = [list(row) for row in rows]
            return rows, columns

    def execute(self, query: str):
        self._check_and_reconnect()
        with self.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

    def _is_table_exists(
            self,
            table_schema: str,
            table_name: str,
    ):
        query = f"""
           select exists (
               select 1 
               from information_schema.tables
               where 1=1
                    and table_schema = '{table_schema}'
                    and table_name = '{table_name}'
               )
           """
        result = self.select(query)
        is_exists = result[0][0]
        return is_exists

    def _create_table(
            self,
            columns: List[str],
            rows: List[List],
            table_name: str,
            table_schema: str,
    ):
        df = pd.DataFrame([rows], columns=columns)
        self.execute(get_schema(df, schema=table_schema, name=table_name))
        print(f"Table '{table_schema.upper()}.{table_name.upper()}' created.")

    def insert_execute_values(
            self,
            columns: List[str],
            rows: List[List],
            table_name: str,
            table_schema: str = "public",
    ):
        self._check_and_reconnect()
        if not self._is_table_exists(table_schema, table_name):
            print(f"Table '{table_schema.upper()}.{table_name.upper()}' doesn't exist, creating...")
            self._create_table(columns, rows[0], table_name,table_schema)

        start = datetime.now()

        with self.conn as conn:
            with conn.cursor() as cursor:
                query = f"INSERT INTO {table_schema}.{table_name} ({', '.join(columns)}) VALUES %s"
                execute_values(cursor, query, rows, page_size=50000)
                duration = datetime.now() - start
                newline = "\n"
                print(
                    f"Data inserted in table: '{table_name.upper()}'"
                    f"{newline}{len(rows):_d} rows inserted"
                    f"{newline}Time: {duration.seconds} seconds"
                )

                total_duration = datetime.now() - start
                logger.warning(
                    f"Pandas job is finished. Total time: {total_duration.seconds} seconds"
                )
