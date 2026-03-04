from typing import Any, Dict, List
from pydantic import BaseModel, Field


class TransformRequest(BaseModel):
    prompt: str
    columns: List[str]
    sample_row: Dict[str, Any] = Field(default_factory=dict)


class AIResponseSchema(BaseModel):
    code: str = Field(description="The executable JavaScript function code")
    explanation: str = Field(description="Brief explanation in Chinese")
