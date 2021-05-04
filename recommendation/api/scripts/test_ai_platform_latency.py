import time

import numpy as np

from google.api_core.client_options import ClientOptions
from googleapiclient import discovery


def predict_score(region, project, model, instances, version):
    endpoint = "https://" + region + "-ml.googleapis.com"
    client_options = ClientOptions(api_endpoint=endpoint)
    service = discovery.build(
        "ml", "v1", client_options=client_options, cache_discovery=False
    )
    name = "projects/" + project + "/models/" + model

    if version is not None:
        name += "/versions/{}".format(version)

    response = (
        service.projects().predict(name=name, body={"instances": instances}).execute()
    )
    if "error" in response:
        raise RuntimeError(response["error"])

    return response["predictions"]


durations = []
for i in range(10):
    start = time.time()
    result = predict_score(
        region="europe-west4",
        project="passculture-data-ehp",
        model="test_44k",
        instances=[{"input_1": [0, 0, 0] * 1000, "input_2": [0, 1, 2] * 1000}],
        version="tf_bpr",
    )
    end = time.time()
    duration = end - start
    durations.append(duration)

# print(result)
print(np.mean(durations))
