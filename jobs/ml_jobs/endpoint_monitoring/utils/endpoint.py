# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START aiplatform_predict_custom_trained_model_sample]

import time
from collections import defaultdict

from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from loguru import logger

from constants import API_ENDPOINT, GCP_PROJECT, LOCATION

RETRIEVAL_SIZE = 60  # Default size for retrieval, can be adjusted as needed


def process_endpoint_calls(endpoint_name, call_type, ids, n_calls_per_user):
    """
    Call the recommendation endpoint for each user in the subset, multiple times.
    Returns predictions, latencies, and success/failure counts.
    """
    predictions_by_id = defaultdict(list)
    latencies = []
    success_count = 0
    failure_count = 0
    for id in ids:
        for call_num in range(n_calls_per_user):
            start_time = time.time()
            results = call_endpoint(
                endpoint_path=get_endpoint_path(endpoint_name=endpoint_name),
                model_type=call_type,
                id=id,
                size=RETRIEVAL_SIZE,
            )
            end_time = time.time()
            latencies.append(end_time - start_time)
            predictions_by_id[id].append(results.predictions)
            if hasattr(results, "predictions") and results.predictions:
                success_count += 1
            else:
                failure_count += 1
            logger.info(f"Call {call_num + 1} completed for user {id}")
            ### search type analysis can be added here if needed
    logger.info(f"Total successful calls: {success_count}")
    logger.info(f"Total failed calls: {failure_count}")
    return predictions_by_id, latencies, success_count, failure_count


def get_endpoint_path(
    endpoint_name: str,
):
    """
    Fetches the endpoint details for a given endpoint name and location.
    Returns a dictionary with model name, model version ID, and endpoint path.
    """

    endpoint = aiplatform.Endpoint.list(
        filter=f"display_name={endpoint_name}", location=LOCATION, project=GCP_PROJECT
    )[0]
    endpoint_dict = endpoint.to_dict()
    return endpoint_dict["name"]


def call_endpoint(endpoint_path: str, model_type: str, id: str, size: int = 10) -> None:
    """
    Example function to call the predict_custom_trained_model_sample.
    This is a placeholder and should be replaced with actual logic.
    """

    instances = {
        "model_type": model_type,
        "user_id": id if model_type == "recommendation" else None,
        "size": size,
        "params": {},
        "call_id": "1234567890",
        "debug": 1,
        "prefilter": 1,
        "similarity_metric": "dot",
    }
    if model_type == "similar_offer":
        instances["items"] = [id]

    response = predict_custom_trained_model_sample(
        project=GCP_PROJECT,
        endpoint_path=endpoint_path,
        instances=instances,
        location=LOCATION,
        api_endpoint=API_ENDPOINT,
    )
    return response


def predict_custom_trained_model_sample(
    project: str,
    endpoint_path: str,
    instances: dict | list[dict],
    location: str = "us-central1",
    api_endpoint: str = "us-central1-aiplatform.googleapis.com",
):
    """
    `instances` can be either single instance of type dict or a list
    of instances.
    """
    # The AI Platform services require regional API endpoints.
    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
    instances = instances if isinstance(instances, list) else [instances]
    instances = [
        json_format.ParseDict(instance_dict, Value()) for instance_dict in instances
    ]
    parameters_dict = {}
    parameters = json_format.ParseDict(parameters_dict, Value())
    # endpoint = client.endpoint_path(
    #     project=project, location=location, endpoint=endpoint_path
    # )
    response = client.predict(
        endpoint=endpoint_path, instances=instances, parameters=parameters
    )
    return response
