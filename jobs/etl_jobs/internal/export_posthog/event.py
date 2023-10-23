from posthog import Posthog
from utils import PostHogEvent


class EventExporter:
    def __init__(self, posthog_api_key, posthog_host, posthog_personal_api_key):

        self.client = Posthog(
            posthog_api_key,
            sync_mode=False,  # force synchron
            host=posthog_host,
            personal_api_key=posthog_personal_api_key,
            thread=32,
        )

    def event_to_posthog(self, event: PostHogEvent) -> None:
        self.client.capture(
            event.device_id,
            event=event.event_type,
            properties={
                **event.properties,
                "firebase_origin": event.origin,
                "environment": event.environment,
                "$geoip_disable": True,
            },
            timestamp=event.timestamp,
            uuid=event.uuid,
        )
