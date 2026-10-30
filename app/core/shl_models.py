from dataclasses import dataclass
from typing import Literal

from app.models.shl_solver import ActionType


DEFAULT_SHL_MODEL = "gemini-3.8-flash"
VERIFY_CODE_COST = 5


@dataclass(frozen=True)
class SHLModelConfig:
    provider: Literal["gemini", "openai"]
    credits: int
    action_type: ActionType


SHL_MODELS = {
    "gemini-3.8-flash": SHLModelConfig("gemini", 10, ActionType.USE_FLASH_MODEL),
    "gemini-4-argon": SHLModelConfig("gemini", 30, ActionType.USE_PRO_MODEL),
    "gemini-3-pro-preview": SHLModelConfig("gemini", 30, ActionType.USE_PRO_MODEL),
    "gemini-2.5-pro": SHLModelConfig("gemini", 30, ActionType.USE_PRO_MODEL),
    "gpt-5.6-sol": SHLModelConfig("openai", 60, ActionType.USE_OPENAI_MODEL),
    "gpt-6-astra": SHLModelConfig("openai", 100, ActionType.USE_OPENAI_MODEL),
}


def get_shl_model_config(key: str) -> SHLModelConfig:
    try:
        return SHL_MODELS[key]
    except KeyError:
        raise ValueError(f"Unsupported SHL model: {key}") from None
