from abc import ABC, abstractmethod

from custom_ingestion.model.config_variables import ConfigVariables
from custom_ingestion.model.parameter import Parameter
from custom_ingestion.service.delta_service import DeltaService
from custom_ingestion.service.s3_service import S3Service


class IncrementalInsertLoadService(ABC):
    def __init__(
        self,
        parameter: Parameter,
        config: ConfigVariables,
        delta_service: DeltaService,
        s3_service: S3Service,
        duck_connection,
    ):
        self.parameter = parameter
        self.config = config
        self.delta_service = delta_service
        self.s3_service = s3_service
        self.duck_connection = duck_connection

    @abstractmethod
    def execute(self, dataframe=None, delta_table=None):
        pass
