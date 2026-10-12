import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

from const import ENDPOINTS, AUDIENCES, BREVO_BASE_URL, TARGET_ENV, GCP_PROJECT
from gcp_secrets import get_brevo_api_key
from patch_for_prism_mock_test import _patch_endpoints_for_mock


def _is_live_mode() -> bool:
    return "api.brevo.com" in BREVO_BASE_URL


# TODO review if default `dlt.secrets.value` is needed
@dlt.source
def brevo_source(api_key: str = dlt.secrets.value):
    resources = ENDPOINTS if _is_live_mode() else _patch_endpoints_for_mock(ENDPOINTS)

    config: RESTAPIConfig = {
        "client": {
            "base_url": BREVO_BASE_URL,
            "auth": {
                "type": "api_key",
                "name": "api-key",
                "api_key": api_key,
                "location": "header",
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
        },
        "resources": resources,
    }

    yield from rest_api_resources(config)


if __name__ == "__main__":
    for audience in AUDIENCES:
        pipeline = dlt.pipeline(
            pipeline_name=f"brevo_{audience}",
            destination="duckdb",
            dataset_name=f"brevo_{audience}",
            dev_mode=True,
            progress="log",
        )

        api_key = get_brevo_api_key(GCP_PROJECT, TARGET_ENV, audience) if _is_live_mode() else "mock-key"

        load_info = pipeline.run(brevo_source(api_key=api_key))

        print(f"[{audience}] {load_info}")
