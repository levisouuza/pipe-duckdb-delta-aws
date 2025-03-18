from custom_ingestion.processor.stage_ingestion_processor import StageIngestionProcessor
from custom_ingestion.processor.bronze_ingestion_processor import BronzeIngestionProcessor
from custom_ingestion.processor.silver_ingestion_processor import SilverIngestionProcessor
from custom_ingestion.processor.gold_dimensions_ingestion_processor import (
    GoldDimensionsIngestionProcessor,
)
from custom_ingestion.processor.gold_facts_ingestion_processor import GoldFactsIngestionProcessor

from custom_ingestion.config.duckdb_config import DuckDbConfig
from custom_ingestion.model.config_variables import ConfigVariables
from custom_ingestion.service.delta_service import DeltaService
from custom_ingestion.service.s3_service import S3Service
from custom_ingestion.service.ssm_service import SsmService


class IngestionProcessor:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._ssm_service = SsmService(self._config)

        self._duckdb = DuckDbConfig(self._config)
        self._duckdb.create_connection_duckdb()
        self._connection = self._duckdb.connection

        self._s3_service = S3Service(self._config)
        self._delta = DeltaService(self._config)

        self.stage_processor = StageIngestionProcessor(self._config, self._s3_service)
        self.bronze_processor = BronzeIngestionProcessor(
            self._config,
            self._ssm_service,
            self._connection,
            self._s3_service,
            self._delta,
        )
        self.silver_processor = SilverIngestionProcessor(
            self._config,
            self._ssm_service,
            self._connection,
            self._s3_service,
            self._delta,
        )
        self.gold_dimensions_processor = GoldDimensionsIngestionProcessor(
            self._config,
            self._ssm_service,
            self._connection,
            self._s3_service,
            self._delta,
        )
        self.gold_facts_processor = GoldFactsIngestionProcessor(
            self._config,
            self._ssm_service,
            self._connection,
            self._s3_service,
            self._delta,
        )
