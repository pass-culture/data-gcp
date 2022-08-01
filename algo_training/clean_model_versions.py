import os
from google.cloud import aiplatform

SERVING_CONTAINER = "europe-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-5:latest"


def clean_model_versions():
    model_id = aiplatform.Model.list(filter=f"display_name={MODEL_NAME}", location=REGION, project=PROJECT_NAME)[0].name

    model = aiplatform.Model(model_name=f"projects/passculture-data-ehp/locations/europe-west1/models/{model_id}")
    ModelRegistry = aiplatform.models.ModelRegistry(
        model,
        REGION,
        PROJECT_NAME,
    )

    versions = ModelRegistry.list_versions()
    if len(versions) < MAX_MODEL_VERSIONS:
        print("SUCCES:versions already clean")
    else:
        versions_to_clean = versions[:-MAX_MODEL_VERSIONS]
        for versions in versions_to_clean:
            ModelRegistry.delete_version(f"{versions.version_id}")

if __name__ == "__main__":
    PROJECT_NAME = os.environ.get("PROJECT_NAME", "")
    REGION = os.environ.get("REGION", "")
    MODEL_NAME = os.environ.get("MODEL_NAME", "")
    MAX_MODEL_VERSIONS = os.environ.get("MAX_MODEL_VERSIONS", "")
    clean_model_versions()
