# app/clients/posthog_client.py
from posthog import Posthog
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

ph_client = None

if settings.posthog_api_key and settings.posthog_host:
    try:
        ph_client = Posthog(
            project_api_key=settings.posthog_api_key, host=settings.posthog_host
        )
        logger.info("PostHog client initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize PostHog client: {e}")


def capture_event(distinct_id: str, event_name: str, properties: dict = None):
    """
    Safely capture a PostHog event.
    """
    if ph_client:
        try:
            ph_client.capture(
                distinct_id, event=event_name, properties=properties or {}
            )
        except Exception as e:
            logger.error(f"PostHog capture error: {e}")


def identify_user(distinct_id: str, properties: dict = None):
    """
    Safely identify a PostHog user.
    """
    if ph_client:
        try:
            ph_client.identify(distinct_id, properties=properties or {})
        except Exception as e:
            logger.error(f"PostHog identify error: {e}")
