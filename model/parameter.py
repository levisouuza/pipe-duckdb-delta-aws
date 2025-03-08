from typing import Optional
from pydantic import BaseModel
from enum import Enum


class IncrementalInsertLoadType(Enum):
    MERGE = "merge"
    SQL = "sql"


class Parameter(BaseModel):
    uri_s3_table: Optional[str] = None
    table_name: Optional[str] = None
    layer: Optional[str] = None
    first_load: Optional[bool] = None
    increment_insert_load_type: Optional[IncrementalInsertLoadType]
    bucket_name: Optional[str] = None
    sql_script_path: Optional[str] = None
    sql_script_path_incremental: Optional[str] = None
    replace_uri: Optional[bool] = False
    incremental_data_df_validate: Optional[bool] = True
