# app/core/config.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    api_key: str
    or_api_key: str
    gemini_api_key: str
    gemini_base_url: str

    # async: FastAPI / SQLAlchemy AsyncSession 使用
    database_url_async: str

    # sync: Alembic / CLI / migration 使用
    database_url_sync: str

    # JWT / Cookie settings for auth
    jwt_secret_key: str = "change_me"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expires_minutes: int = 60
    jwt_refresh_token_expires_days: int = 7

    cookie_secure: bool = False
    cookie_samesite: str = "lax"
    cookie_httponly: bool = True
    # Redis settings
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 1
    redis_username: str | None = None
    redis_password: str | None = None

    # Email settings
    sender_email: str | None = None
    sender_password: str | None = None
    smtp_server: str = "smtp.qq.com"
    smtp_port: int = 465

    # FF14
    ff14_api_key: str
    ff14_base_url: str
    ff14_v2_client_id: str
    ff14_v2_secret_key: str
    ff14_v2_redirect_uri: str
    ff14_v2_authorize_url: str = "https://www.fflogs.com/oauth/authorize"
    ff14_v2_token_url: str = "https://www.fflogs.com/oauth/token"
    ff14_v2_client_base_url: str = "https://www.fflogs.com/api/v2/client"
    ff14_v2_user_base_url: str = "https://www.fflogs.com/api/v2/user"

    # Twelve Data
    twelve_data_api_key: str
    twelve_data_base_url: str

    # FXCM sidecar
    fxcm_api_base_url: str = "http://127.0.0.1:8100"
    fxcm_api_timeout_seconds: float = 60.0

    # PostHog
    posthog_api_key: str
    posthog_host: str

    frontend_base_url: str = "http://localhost:3000"

    class Config:
        env_file = ".env"


settings = Settings()
