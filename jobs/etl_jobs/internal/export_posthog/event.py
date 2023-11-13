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
        if event.event_type in ("page_view", "screen_view"):
            self.page(event)
        else:
            self.capture(event)

    def page(self, event: PostHogEvent) -> None:
        self.client.page(
            event.device_id,
            url=event.screen,
            properties=dict(event.properties, **event.user_properties),
            timestamp=event.timestamp,
            uuid=event.uuid,
            disable_geoip=True,
        )

    def capture(self, event: PostHogEvent) -> None:
        self.client.capture(
            event.device_id,
            event=event.event_type,
            properties={
                **event.properties,
                "$geoip_disable": True,
                "$set": {**event.user_properties},
            },
            timestamp=event.timestamp,
            uuid=event.uuid,
        )
