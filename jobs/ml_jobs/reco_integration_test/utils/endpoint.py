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

from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value


def call_endpoint(model_type: str, id: str, size: int = 10) -> None:
    """
    Example function to call the predict_custom_trained_model_sample.
    This is a placeholder and should be replaced with actual logic.
    """
    PROJECT = "962488981524"
    ENDPOINT_ID = "4777694682035519488"
    LOCATION = "europe-west1"
    API_ENDPOINT = "europe-west1-aiplatform.googleapis.com"

    instances = {
        "model_type": model_type,
        "user_id": id if model_type == "recommendation" else None,
        "item_id": id if model_type == "similar_offers" else None,
        "size": size,
        "params": {},
        "call_id": "1234567890",
        "debug": 1,
        "prefilter": 1,
        "similarity_metric": "dot",
    }

    response = predict_custom_trained_model_sample(
        project=PROJECT,
        endpoint_id=ENDPOINT_ID,
        instances=instances,
        location=LOCATION,
        api_endpoint=API_ENDPOINT,
    )
    return response


def predict_custom_trained_model_sample(
    project: str,
    endpoint_id: str,
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
    endpoint = client.endpoint_path(
        project=project, location=location, endpoint=endpoint_id
    )
    response = client.predict(
        endpoint=endpoint, instances=instances, parameters=parameters
    )
    return response
