import duckdb
from model.config_variables import ConfigVariables

class DuckDbConfig:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self.connection = None

    def create_connection_duckdb(self):
        self.connection = duckdb.connect(database=":memory:")

        self.connection.execute("SET home_directory='../duckdb';")

        self._install_and_load_extension()
        self._configs_http_endpoint()

        self.connection.execute(f"""
            CREATE SECRET my_s3_secret (
                    TYPE 'S3',
                     KEY_ID '{self._config.aws_access_key_id}',
                     SECRET '{self._config.aws_secret_access_key}',
                     REGION '{self._config.aws_region}'
                );
        """)

    def close_connection(self):
        self.connection.close()


    def _install_and_load_extension(self):
        for extension in self._config.extension_duck:
            self.connection.execute(f"INSTALL {extension};")
            self.connection.execute(f"LOAD {extension};")

    def _configs_http_endpoint(self):
        self.connection.execute(f"SET s3_endpoint='{self._config.s3_endpoint}';")
        self.connection.execute(f"SET http_retries = {self._config.http_retries};")
        self.connection.execute(f"SET http_retry_backoff = {self._config.http_retry_backoff};")
