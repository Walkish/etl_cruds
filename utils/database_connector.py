from typing import Union


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

    def get_conn_string(self, database: str):
        return f"{database}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
