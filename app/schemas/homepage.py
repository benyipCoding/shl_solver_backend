from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class PublicHomepageConfig(BaseModel):
    active_version: Literal[1, 2]


class AdminHomepageConfig(PublicHomepageConfig):
    revision: int
    updated_at: datetime
    updated_by: int | None


class PublishHomepageRequest(BaseModel):
    active_version: Literal[1, 2]
    expected_revision: int = Field(strict=True, ge=0)

    @field_validator("active_version", mode="before")
    @classmethod
    def require_integer_version(cls, value):
        if type(value) is not int:
            raise ValueError("首页版本必须是整数 1 或 2")
        return value
