import os
from google.cloud import aiplatform

SERVING_CONTAINER = "europe-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-5:latest"


def upload_model_and_deploy_to_endpoint():

    print('Uploading model to Vertex AI model registery...')
    parent_model_id = aiplatform.Model.list(
        filter=f"display_name={MODEL_NAME}", location=REGION, project=PROJECT_NAME
    )[0].name

    model = aiplatform.Model.upload(
        display_name=VERSION_NAME,
        project=PROJECT_NAME,
        artifact_uri=RECOMMENDATION_MODEL_DIR,
        serving_container_image_uri=SERVING_CONTAINER,
        parent_model=parent_model_id,
        location="europe-west1",
    )

    endpoint = aiplatform.Endpoint.list(filter=f"display_name={END_POINT_NAME}", location=REGION, project=PROJECT_NAME)[0]
    print('Deploy model to endpoint...')
    model.deploy(
        endpoint=endpoint,
        deployed_model_display_name=VERSION_NAME,
        traffic_percentage=100,
    )
    model.wait()

    print("Undeploy old versions..")
    #Problem get old deployed_model info 
    deployed_model_id="4842372354027814912"#info contained in object endpoint 
    endpoint.undeploy(deployed_model_id)


if __name__ == "__main__":
    REGION = os.environ.get("REGION", "")
    PROJECT_NAME = os.environ.get("PROJECT_NAME", "")
    MODEL_NAME = os.environ.get("MODEL_NAME", "")
    VERSION_NAME = os.environ.get("VERSION_NAME", "")
    RECOMMENDATION_MODEL_DIR = os.environ.get("RECOMMENDATION_MODEL_DIR", "")
    END_POINT_NAME = os.environ.get("END_POINT_NAME", "")
    upload_model_and_deploy_to_endpoint()
