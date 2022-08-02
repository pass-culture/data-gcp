from typing import Dict, List, Union

from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from pcreco.utils.env_vars import ENV_SHORT_NAME, GCP_PROJECT

END_POINT_NAME = f"vertex_ai_{ENV_SHORT_NAME}"


def predict_custom_trained_model_sample(
    project: str,
    endpoint_id: str,
    instances: Union[Dict, List[Dict]],
    location: str = "europe-west1",
    api_endpoint: str = "europe-west1-aiplatform.googleapis.com",
):
    """
    `instances` can be either single instance of type dict or a list
    of instances.
    """
    # The AI Platform services require regional API endpoints.
    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    # This client only needs to be created once, and can be reused for multiple requests.
    client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
    # The format of each instance should conform to the deployed model's prediction input schema.
    instances = instances if type(instances) == list else [instances]
    instances = [
        json_format.ParseDict(instance_dict, Value()) for instance_dict in instances
    ]
    parameters_dict = {}
    parameters = json_format.ParseDict(parameters_dict, Value())
    endpoint_path = client.endpoint_path(
        project=project, location=location, endpoint=endpoint_id
    )
    response = client.predict(
        endpoint=endpoint_path, instances=instances, parameters=parameters
    )
    endpoint = aiplatform.Endpoint.list(
        filter=f"display_name={END_POINT_NAME}", location=location, project=GCP_PROJECT
    )[0]
    endpoint_dict = endpoint.to_dict()
    version_model_id = endpoint_dict["deployedModels"][0]["displayName"]
    response_dict = {
        "predictions": response.predictions,
        "model_version_id": version_model_id,
        "model_display_name": response.model_display_name,
    }
    return response_dict
