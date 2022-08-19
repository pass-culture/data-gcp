import os
from google.cloud import aiplatform

SERVING_CONTAINER = "europe-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-5:latest"


def upload_model_and_deploy_to_endpoint(
    region,
    project_name,
    model_name,
    version_name,
    recommendation_model_dir,
    end_point_name,
    min_nodes,
    max_nodes,
):

    print("Uploading model to Vertex AI model registery...")
    parent_model_id = aiplatform.Model.list(
        filter=f"display_name={model_name}", location=region, project=project_name
    )[0].name

    model = aiplatform.Model.upload(
        display_name=version_name,
        project=project_name,
        artifact_uri=recommendation_model_dir,
        serving_container_image_uri=SERVING_CONTAINER,
        parent_model=parent_model_id,
        location="europe-west1",
    )

    endpoint = aiplatform.Endpoint.list(
        filter=f"display_name={end_point_name}", location=region, project=project_name
    )[0]
    print("Deploy model to endpoint...")
    model.deploy(
        endpoint=endpoint,
        deployed_model_display_name=version_name,
        min_replica_count=min_nodes,
        max_replica_count=max_nodes,
        traffic_percentage=100,
    )
    model.wait()

    print("Undeploy old versions..")
    endpoint_dict = endpoint.to_dict()
    deployed_models_sorted_by_date = sorted(
        endpoint_dict["deployedModels"], key=lambda d: d["createTime"]
    )
    previous_version_model_id = deployed_models_sorted_by_date[0]["id"]
    endpoint.undeploy(previous_version_model_id)


if __name__ == "__main__":
    REGION = os.environ.get("REGION", "")
    PROJECT_NAME = os.environ.get("PROJECT_NAME", "")
    MODEL_NAME = os.environ.get("MODEL_NAME", "")
    VERSION_NAME = os.environ.get("VERSION_NAME", "")
    RECOMMENDATION_MODEL_DIR = os.environ.get("RECOMMENDATION_MODEL_DIR", "")
    END_POINT_NAME = os.environ.get("END_POINT_NAME", "")
    try:
        MIN_NODES = int(os.environ.get("MIN_NODES", ""))
        MAX_NODES = int(os.environ.get("MAX_NODES", ""))
    except:
        MIN_NODES = 1
        MAX_NODES = 1
    upload_model_and_deploy_to_endpoint(
        REGION,
        PROJECT_NAME,
        MODEL_NAME,
        VERSION_NAME,
        RECOMMENDATION_MODEL_DIR,
        END_POINT_NAME,
        MIN_NODES,
        MAX_NODES,
    )
