from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from google.api_core.exceptions import DeadlineExceeded
from typing import Dict, List, Union
from cachetools import cached, TTLCache
from dataclasses import dataclass
import concurrent.futures
from functools import partial
import grpc

from huggy.utils.env_vars import GCP_PROJECT


@dataclass
class PredictionResult:
    status: str
    predictions: List[str]
    model_version: str
    model_display_name: str


@cached(cache=TTLCache(maxsize=1024, ttl=120))
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


def parallel_endpoint_score(endpoint_name, instances):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        func = partial(endpoint_score, endpoint_name)
        futures = [executor.submit(func, inst) for inst in instances]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    return results


def endpoint_score(endpoint_name, instances, fallback_endpoints=[]) -> PredictionResult:
    for endpoint in [endpoint_name] + fallback_endpoints:
        response = __predict_model(
            endpoint_name=endpoint,
            location="europe-west1",
            instances=instances,
        )
        prediction_result = PredictionResult(
            status=response["status"],
            predictions=response["predictions"],
            model_display_name=response["model_display_name"],
            model_version=response["model_version_id"],
        )
        # if we have results, return, else fallback_endpoints
        if len(response["predictions"]) > 0:
            return prediction_result
    # default
    return prediction_result


def __predict_model(
    endpoint_name: str,
    instances: Union[Dict, List[Dict]],
    location: str = "europe-west1",
    api_endpoint: str = "europe-west1-aiplatform.googleapis.com",
):
    """
    `instances` can be either single instance of type dict or a list
    of instances.
    """
    default_error = {
        "status": "success",
        "predictions": [],
        "model_version_id": "unknown",
        "model_display_name": "unknown",
    }
    try:
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
        try:
            response = client.predict(
                endpoint=model_params["endpoint_path"],
                instances=instances,
                parameters=parameters,
                timeout=1,
            )
        except DeadlineExceeded:
            return {
                "status": "error",
                "predictions": [],
                "model_version_id": model_params["model_version_id"],
                "model_display_name": model_params["model_name"],
            }

        response_dict = {
            "status": "success",
            "predictions": response.predictions,
            "model_version_id": model_params["model_version_id"],
            "model_display_name": model_params["model_name"],
        }
    except grpc._channel._InactiveRpcError as e:
        return default_error
    except Exception as e:
        return default_error

    return response_dict
