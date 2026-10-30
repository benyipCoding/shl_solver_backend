from typing import Any, Dict, List, Literal
from pydantic import BaseModel, Field


class TransformRequest(BaseModel):
    prompt: str
    columns: List[str]
    sample_row: Dict[str, Any] = Field(default_factory=dict)
    protocol_version: Literal[1, 2] = 1


class AIResponseSchema(BaseModel):
    code: str = Field(description="Pure JavaScript transform function for local data and style editing")
    explanation: str = Field(description="Brief explanation in Chinese")
