import os
from google.cloud import aiplatform


def clean_model_versions(project_name, region, model_name, max_model_versions):
    model_id = aiplatform.Model.list(
        filter=f"display_name={model_name}", location=region, project=project_name
    )[0].name

    model = aiplatform.Model(
        model_name=f"projects/{project_name}/locations/europe-west1/models/{model_id}"
    )
    ModelRegistry = aiplatform.models.ModelRegistry(
        model,
        region,
        project_name,
    )

    versions = ModelRegistry.list_versions()
    if len(versions) < max_model_versions:
        print("SUCCES:versions already clean")
    else:
        versions_to_clean = versions[:-max_model_versions]
        for versions in versions_to_clean:
            ModelRegistry.delete_version(f"{versions.version_id}")


if __name__ == "__main__":
    PROJECT_NAME = os.environ.get("PROJECT_NAME", "")
    REGION = os.environ.get("REGION", "")
    MODEL_NAME = os.environ.get("MODEL_NAME", "")
    ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "")
    MAX_MODEL_VERSIONS = 5 if ENV_SHORT_NAME == "prod" else 1

    clean_model_versions(PROJECT_NAME, REGION, MODEL_NAME, MAX_MODEL_VERSIONS)
