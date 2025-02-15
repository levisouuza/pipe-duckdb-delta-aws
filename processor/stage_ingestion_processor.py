from service.s3_service import S3Service
from model.config_variables import ConfigVariables
from utils.files_utils import get_files_in_raw_datasets


class StageIngestionProcessor:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._s3_service = S3Service(self._config)

    def put_files_in_stage_bucket(self):
        s3_files_to_ingestion_list = get_files_in_raw_datasets()

        for s3_files_to_ingestion in s3_files_to_ingestion_list:
            print(s3_files_to_ingestion)
            self._s3_service.upload_file_s3(
                self._config.buckets.stage,
                s3_files_to_ingestion.get("local_filename"),
                s3_files_to_ingestion.get("s3_filename"),
            )


_config = ConfigVariables()  # noqa
stage_processor = StageIngestionProcessor(_config)
stage_processor.put_files_in_stage_bucket()
