from openai import AsyncOpenAI

from app.core.config import settings


_client: AsyncOpenAI | None = None


def init_openai_client():
    global _client
    base_url = settings.openai_base_url.rstrip("/")
    if not base_url.endswith("/v1"):
        base_url += "/v1"
    _client = AsyncOpenAI(base_url=base_url, api_key=settings.api_key)


def get_openai_client() -> AsyncOpenAI:
    if _client is None:
        raise RuntimeError("OpenAI client not initialized")
    return _client


async def close_openai_client():
    global _client
    if _client is not None:
        await _client.close()
        _client = None
