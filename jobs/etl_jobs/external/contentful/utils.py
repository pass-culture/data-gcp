import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

GCP_PROJECT = os.environ.get("GCP_PROJECT")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


PREVIEW_TOKEN = access_secret_data(GCP_PROJECT, "contentful-preview-token")
TOKEN = access_secret_data(GCP_PROJECT, "contentful-token")
SPACE_ID = access_secret_data(GCP_PROJECT, "contentful-space-id")

ENTRIES_DTYPE = {
    "title": str,
    "contentful_tags_id": str,
    "contentful_tags_name": str,
    "space": str,
    "id": str,
    "type": str,
    "environment": str,
    "content_type": str,
    "locale": str,
    "modules": str,
    "display_parameters": str,
    "venues_search_parameters": str,
    "around_radius": str,
    "hits_per_page": str,
    "is_geolocated": str,
    "tags": str,
    "venue_types": str,
    "layout": str,
    "min_offers": str,
    "additional_algolia_parameters": float,
    "algolia_parameters": str,
    "cover": str,
    "beginning_datetime": str,
    "categories": str,
    "ending_datetime": str,
    "image_full_screen": float,
    "is_digital": str,
    "is_duo": str,
    "is_event": str,
    "is_thing": str,
    "newest_only": str,
    "price_max": str,
    "price_min": str,
    "subcategories": str,
    "first_line": str,
    "image": str,
    "left_icon": str,
    "second_line": str,
    "target_not_connected_users_only": str,
    "url": str,
    "alt": str,
    "offer_id": str,
    "highlight_title": str,
    "offer_image": str,
    "offer_tag": str,
    "offer_title": str,
    "category_block_list": str,
    "home_entry_id": str,
    "recommendation_parameters": str,
    "thematic_highlight_info": str,
    "thematic_home_entry_id": str,
    "color": str,
    "displayed_title": str,
    "duration_in_minutes": float,
    "video_description": str,
    "video_tag": str,
    "video_title": str,
    "youtube_video_id": str,
    "items": str,
    "tag": str,
    "trend_block": float,
    "video_carousel_item": float,
    "is_free": str,
    "offer_ean": str,
    "gtl_label": str,
    "gtl_level": str,
    "revision": int,
}
