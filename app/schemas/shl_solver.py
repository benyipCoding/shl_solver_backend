from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, BeforeValidator
from typing_extensions import Annotated
import json


# Helper to parse JSON string to dict
def parse_json_field(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (json.JSONDecodeError, TypeError):
            return {}
    return v


# Helper to parse comma-separated string to list
def parse_comma_separated_list(v: Any) -> List[str]:
    if isinstance(v, str):
        return [url.strip() for url in v.split(",") if url.strip()]
    return v


JsonData = Annotated[Dict[str, Any], BeforeValidator(parse_json_field)]
StringList = Annotated[List[str], BeforeValidator(parse_comma_separated_list)]


class SHLSolverHistorySerializer(BaseModel):
    id: int
    image_urls: StringList = Field(..., description="List of image URLs")
    token_count: int
    model: Optional[str] = None
    username: str = Field(
        ..., description="Username of the user who initiated the task"
    )
    result_json: Optional[JsonData] = Field(None, description="Parsed JSON result")
    total_test_cases: int
    passed_test_cases: int
    status: str
    error_message: Optional[str] = None
    is_readed: Optional[bool] = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class SHLSolverHistoryListResponse(BaseModel):
    items: List[SHLSolverHistorySerializer]
    total: int
    page: int
    size: int


class SHLSolverHistoryPatch(BaseModel):
    is_readed: Optional[bool] = None
