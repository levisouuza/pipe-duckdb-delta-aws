from abc import ABC, abstractmethod

from model.config_variables import ConfigVariables
from model.parameter import Parameter
from service.delta_service import DeltaService
from service.s3_service import S3Service


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
