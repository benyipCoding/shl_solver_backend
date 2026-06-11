# app/core/config.py
from urllib.parse import urlparse

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_env: str = "prod"

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

    # FXCM market data sync MVP
    fxcm_sync_enabled: bool = True
    fxcm_sync_hot_symbols: str = (
        "EUR/USD,GBP/USD,USD/JPY,AUD/USD,USD/CAD,USD/CHF,"
        "BTC/USD,ETH/USD,XAU/USD,XAG/USD,USOil,UKOil,"
        "US30,NAS100,SPX500,USDOLLAR"
    )
    fxcm_sync_intervals: str = "30min,1h,2h,4h,1day"
    fxcm_sync_backfill_bars: int = 1000
    fxcm_sync_metadata_interval_hours: int = 12
    fxcm_sync_poll_interval_seconds: int = 60
    fxcm_sync_batch_size: int = 8
    fxcm_sync_incremental_overlap_bars: int = 2
    fxcm_sync_1h_incremental_outputsize: int = 200
    fxcm_sync_1day_incremental_outputsize: int = 60

    # PostHog
    posthog_api_key: str
    posthog_host: str

    frontend_base_url: str = "http://localhost:3000"

    @property
    def is_local_development(self) -> bool:
        normalized_env = self.app_env.strip().casefold()
        if normalized_env in {"local", "dev", "development"}:
            return True
        if normalized_env in {"production", "prod", "staging"}:
            return False

        fxcm_host = (urlparse(self.fxcm_api_base_url).hostname or "").casefold()
        frontend_host = (urlparse(self.frontend_base_url).hostname or "").casefold()
        local_hosts = {"localhost", "127.0.0.1", "0.0.0.0"}
        return fxcm_host in local_hosts and frontend_host in local_hosts

    class Config:
        env_file = ".env"


settings = Settings()
