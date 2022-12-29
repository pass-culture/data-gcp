from typing import Dict, List, Union

from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from pcreco.utils.env_vars import GCP_PROJECT
from cachetools import cached, TTLCache


@cached(cache=TTLCache(maxsize=1024, ttl=60))
def get_model(endpoint_name, location):
    return __get_model(endpoint_name, location)


def __get_model(endpoint_name, location):
    endpoint = aiplatform.Endpoint.list(
        filter=f"display_name={endpoint_name}", location=location, project=GCP_PROJECT
    )[0]
    endpoint_dict = endpoint.to_dict()
    return {
        "model_name": endpoint_dict["displayName"],
        "model_version_id": endpoint_dict["deployedModels"][0]["displayName"],
        "endpoint_path": endpoint_dict["name"],
    }


@cached(cache=TTLCache(maxsize=1024, ttl=600))
def get_client(api_endpoint):
    client_options = {"api_endpoint": api_endpoint}
    return aiplatform.gapic.PredictionServiceClient(client_options=client_options)


def predict_model(
    endpoint_name: str,
    instances: Union[Dict, List[Dict]],
    location: str = "europe-west1",
    api_endpoint: str = "europe-west1-aiplatform.googleapis.com",
):
    """
    `instances` can be either single instance of type dict or a list
    of instances.
    """

    client = get_client(api_endpoint)
    try:
        model_params = get_model(endpoint_name, location)
    # TODO fix this
    except:
        model_params = __get_model(endpoint_name, location)

    instances = instances if type(instances) == list else [instances]
    instances = [
        json_format.ParseDict(instance_dict, Value()) for instance_dict in instances
    ]
    parameters_dict = {}
    parameters = json_format.ParseDict(parameters_dict, Value())
    response = client.predict(
        endpoint=model_params["endpoint_path"],
        instances=instances,
        parameters=parameters,
    )

    response_dict = {
        "predictions": response.predictions,
        "model_version_id": model_params["model_version_id"],
        "model_display_name": model_params["model_name"],
    }
    return response_dict
