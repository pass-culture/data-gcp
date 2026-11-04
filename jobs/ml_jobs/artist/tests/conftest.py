from unittest import mock

from pydantic_ai.models.test import TestModel

# src/similarity/llm_config.py instantiates the Gemini model at import time, which
# resolves GCP application default credentials. Patch both classes before any test
# module imports it, so LLM_MODEL becomes a local TestModel and nothing reaches GCP.
mock.patch(
    "pydantic_ai.providers.google_cloud.GoogleCloudProvider", autospec=True
).start()
mock.patch("pydantic_ai.models.google.GoogleModel", return_value=TestModel()).start()
