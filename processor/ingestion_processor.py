from processor.stage_ingestion_processor import StageIngestionProcessor
from processor.bronze_ingestion_processor import BronzeIngestionProcessor
from processor.silver_ingestion_processor import SilverIngestionProcessor
from processor.gold_dimensions_ingestion_processor import (
    GoldDimensionsIngestionProcessor,
)
from processor.gold_facts_ingestion_processor import GoldFactsIngestionProcessor

from config.duckdb_config import DuckDbConfig
from model.config_variables import ConfigVariables
from service.delta_service import DeltaService
from service.s3_service import S3Service
from service.ssm_service import SsmService


class IngestionProcessor:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._ssm_service = SsmService(self._config)

        self._duckdb = DuckDbConfig(self._config)
        self._duckdb.create_connection_duckdb()
        self._connection = self._duckdb.connection

        self._s3_service = S3Service(self._config)
        self._delta = DeltaService(self._config)

        self._stage_processor = StageIngestionProcessor(self._config, self._s3_service)
        self._bronze_processor = BronzeIngestionProcessor(
            self._config,
            self._ssm_service,
            self._connection,
            self._s3_service,
            self._delta,
        )
        self._silver_processor = SilverIngestionProcessor(
            self._config,
            self._ssm_service,
            self._connection,
            self._s3_service,
            self._delta,
        )
        self._gold_dimensions_processor = GoldDimensionsIngestionProcessor(
            self._config,
            self._ssm_service,
            self._connection,
            self._s3_service,
            self._delta,
        )
        self._gold_facts_processor = GoldFactsIngestionProcessor(
            self._config,
            self._ssm_service,
            self._connection,
            self._s3_service,
            self._delta,
        )

    def process(self):
        self._stage_processor.put_files_in_stage_bucket()
        self._bronze_processor.write_delta_bronze_layer()
        self._silver_processor.write_delta_silver_layer()
        self._gold_dimensions_processor.write_delta_gold_layer()
        self._gold_facts_processor.write_delta_gold_layer()
