import duckdb
from model.config_variables import ConfigVariables

class DuckDbConfig:
    def __init__(self, config: ConfigVariables):
        self._config = config

    def create_connection_duckdb(self):
        conn = duckdb.connect(database=":memory:")

        conn.execute("SET home_directory='../duckdb';")

        self._install_and_load_extension(conn)
        self._configs_http_endpoint(conn)

        conn.execute(f"""
            CREATE SECRET my_s3_secret (
                    TYPE 'S3',
                     KEY_ID '{self._config.aws_access_key_id}',
                     SECRET '{self._config.aws_secret_access_key}',
                     REGION '{self._config.aws_region}'
                );
        """)

        return conn


    def _install_and_load_extension(self, conn):
        for extension in self._config.extension_duck:
            conn.execute(f"INSTALL {extension};")
            conn.execute(f"LOAD {extension};")

    def _configs_http_endpoint(self, conn):
        conn.execute(f"SET s3_endpoint={self._config.s3_endpoint};")
        conn.execute(f"SET http_retries = {self._config.http_retries};")
        conn.execute(f"SET http_retry_backoff = {self._config.http_retry_backoff};")
