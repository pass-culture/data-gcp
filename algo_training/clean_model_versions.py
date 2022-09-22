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
    project_name = os.environ.get("GCP_PROJECT_ID", "")
    region = os.environ.get("REGION", "")
    model_name = os.environ.get("MODEL_NAME", "")
    env_short_name = os.environ.get("ENV_SHORT_NAME", "")
    max_model_versions = 5 if env_short_name == "prod" else 1

    clean_model_versions(project_name, region, model_name, max_model_versions)
