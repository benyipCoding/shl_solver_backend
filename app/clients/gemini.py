from google import genai
from app.core.config import settings

_client: genai.Client | None = None


def init_gemini_client():
    global _client
    _client = genai.Client(
        api_key=settings.api_key,
        vertexai=True,
        http_options={"base_url": settings.gemini_base_url},
    )


def get_gemini_client() -> genai.Client:
    if _client is None:
        raise RuntimeError("Gemini client not initialized")
    return _client
