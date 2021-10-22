import os
from datetime import date
from google.cloud import secretmanager
from google.auth.exceptions import DefaultCredentialsError

from scripts import qpi_downloader


def access_secret(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


project_name = os.environ["PROJECT_NAME"]
TYPEFORM_API_KEY = access_secret(project_name, "typeform_api_key")
if os.environ["ENV"] == "prod":
    FORM_ID = "HGBAFB"
elif os.environ["ENV"] == "stg":
    FORM_ID = "WCBwrmqL"
else:
    FORM_ID = "OAzQTppn"
BUCKET_NAME = os.environ["BUCKET_NAME"]
STORAGE_PATH = BUCKET_NAME + "/QPI_exports/"


def run(request):
    """The Cloud Function entrypoint.
    Args:
        request (flask.Request): The request object.
    """
    today = date.today().strftime("%Y%m%d")
    answers_file_name = STORAGE_PATH + f"qpi_answers_{today}.jsonl"
    questions_file_name = STORAGE_PATH + f"qpi_questions_{today}.csv"

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and "after" in request_json:
        after = request_json["after"]
    elif request_args and "after" in request_args:
        after = request_args["after"]
    else:
        raise RuntimeError("You need to provide an after argument.")

    downloader = qpi_downloader.QPIDownloader(
        TYPEFORM_API_KEY,
        FORM_ID,
        answers_file_name,
        questions_file_name,
        project_name,
        after,
    )

    downloader.run()

    return "Done"
