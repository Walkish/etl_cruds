import psycopg2
from psycopg2.extras import execute_values
from typing import List, Union
from datetime import datetime


class DBConnector:
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

    def get_conn_string(self, database: str):
        return f"{database}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

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

    def insert(
        self,
        columns: List[str],
        rows: List[List],
        table_name: str,
        table_schema: str = "public",
    ):
        self._check_and_reconnect()
        with self.conn as conn:
            with conn.cursor() as cursor:
                query = f"INSERT INTO {table_schema}.{table_name} ({', '.join(columns)}) VALUES %s"
                execute_values(cursor, query, rows, page_size=50000)
