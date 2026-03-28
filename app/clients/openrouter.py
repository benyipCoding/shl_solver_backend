# app/clients/gemini.py
from openai import AsyncOpenAI
from app.core.config import settings

_client: AsyncOpenAI | None = None


def init_openrouter_client():
    global _client
    _client = AsyncOpenAI(
        api_key=settings.or_api_key,  # 换成 OpenRouter 密钥
        base_url="https://openrouter.ai/api/v1",
    )


def get_openrouter_client() -> AsyncOpenAI:
    if _client is None:
        raise RuntimeError("Client not initialized")
    return _client
