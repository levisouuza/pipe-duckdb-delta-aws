from typing import Optional
from pydantic import BaseModel

class Parameter(BaseModel):
    table_name: Optional[str]
    first_load: Optional[bool]
    increment_load_type: Optional[str]