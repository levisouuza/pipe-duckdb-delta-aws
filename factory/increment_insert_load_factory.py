from model.parameter import IncrementalInsertLoadType
from model.config_variables import ConfigVariables
from model.parameter import Parameter
from service.incremental_insert_load_service import IncrementalInsertLoadService
from service.incremental_insert_load_merge_service import (
    IncrementalInsertLoadMergeService,
)
from service.incremental_insert_load_sql_service import IncrementalInsertLoadSqlService
from service.delta_service import DeltaService
from service.s3_service import S3Service


class IncrementInsertLoadFactory:
    _increment_insert_load_type = {
        IncrementalInsertLoadType.MERGE: IncrementalInsertLoadMergeService,
        IncrementalInsertLoadType.SQL: IncrementalInsertLoadSqlService,
    }

    @classmethod
    def get_increment_insert_load_service(
        cls, parameter: Parameter, config: ConfigVariables, delta_service: DeltaService, s3_service: S3Service, duck_connection
    ) -> IncrementalInsertLoadService:
        if parameter.increment_insert_load_type in cls._increment_insert_load_type:
            return cls._increment_insert_load_type.get(
                parameter.increment_insert_load_type
            )(parameter, config, delta_service, s3_service, duck_connection)
        raise ValueError(
            f"Unsupported Incremental Insert Load Type: {parameter.increment_insert_load_type}"
        )
