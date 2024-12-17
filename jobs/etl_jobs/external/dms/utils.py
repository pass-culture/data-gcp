import json

import gcsfs
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

from constants import GCP_PROJECT_ID


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def mergeDictionary(dict_1, dict_2):
    dict_3 = {**dict_1, **dict_2}
    for key, value in dict_3.items():
        if key in dict_1 and key in dict_2:
            if isinstance(dict_1[key], list):
                list_value = [value]
                dict_3[key] = list_value + dict_1[key]
            else:
                dict_3[key] = [value, dict_1[key]]
    return dict_3


def save_json(json_object, filename):
    fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
    with fs.open(filename, "w") as json_file:
        json_file.write(json.dumps(json_object))
    result = filename + " upload complete"
    return {"response": result}
