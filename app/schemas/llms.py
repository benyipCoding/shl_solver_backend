# app/schemas/llms.py
from pydantic import BaseModel, computed_field
from app.core.shl_models import DEFAULT_SHL_MODEL, SHL_MODELS


class LLMSerializer(BaseModel):
    id: int
    key: str
    name: str
    tag: str
    desc: str
    enabled: bool

    @computed_field
    @property
    def provider(self) -> str | None:
        config = SHL_MODELS.get(self.key)
        return config.provider if config else ("gemini" if self.key.startswith("gemini-") else None)

    @computed_field
    @property
    def cost_credits(self) -> int | None:
        config = SHL_MODELS.get(self.key)
        return config.credits if config else None

    @computed_field
    @property
    def is_default(self) -> bool:
        return self.key == DEFAULT_SHL_MODEL

    model_config = {"from_attributes": True}  # ⭐ Pydantic v2 关键
