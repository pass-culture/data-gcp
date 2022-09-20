import os
from google.cloud import aiplatform


def upload_model_and_deploy_to_endpoint(
    region,
    project_name,
    serving_container,
    model_name,
    model_description,
    version_name,
    recommendation_model_dir,
    end_point_name,
    min_nodes,
    max_nodes,
    serving_container_predict_route,
    serving_container_health_route,
    serving_container_ports,
):

    print("Uploading model to Vertex AI model registery...")
    parent_model_id = aiplatform.Model.list(
        filter=f"display_name={model_name}", location=region, project=project_name
    )[0].name

    model = aiplatform.Model.upload(
        display_name=version_name,
        project=project_name,
        artifact_uri=recommendation_model_dir,
        serving_container_image_uri=serving_container,
        parent_model=parent_model_id,
        serving_container_predict_route=serving_container_predict_route,
        serving_container_health_route=serving_container_health_route,
        serving_container_ports=serving_container_ports,
        description=model_description,
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
    region = os.environ.get("REGION", "")
    project_name = os.environ.get("PROJECT_NAME", "")
    model_name = os.environ.get("MODEL_NAME", "")
    model_type = os.environ.get("MODEL_TYPE", "tensorflow")
    model_description = os.environ.get("MODEL_DESCRIPTION", "")

    version_name = os.environ.get("VERSION_NAME", "")
    serving_container = os.environ.get("SERVING_CONTAINER", "")
    recommendation_model_dir = os.environ.get("RECOMMENDATION_MODEL_DIR", None)
    end_point_name = os.environ.get("END_POINT_NAME", "")

    if model_type == "custom":
        serving_container_predict_route = "/predict"
        serving_container_health_route = "/isalive"
        serving_container_ports = [8080]
    else:
        serving_container_predict_route = None
        serving_container_health_route = None
        serving_container_ports = None

    try:
        min_nodes = int(os.environ.get("MIN_NODES", ""))
        max_nodes = int(os.environ.get("MAX_NODES", ""))
    except:
        min_nodes = 1
        max_nodes = 1
    upload_model_and_deploy_to_endpoint(
        region,
        project_name,
        serving_container,
        model_name,
        model_description,
        version_name,
        recommendation_model_dir,
        end_point_name,
        min_nodes,
        max_nodes,
        serving_container_predict_route,
        serving_container_health_route,
        serving_container_ports,
    )
