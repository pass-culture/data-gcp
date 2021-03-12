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


project_id = os.environ["PROJECT_NAME"]
TYPEFORM_API_KEY = access_secret(project_id, "typeform_api_key")
NEW_PROD_FORM_ID = "HGBAFB"
BUCKET_NAME = os.environ["BUCKET_NAME"]
STORAGE_PATH = BUCKET_NAME + "/QPI_exports/"


def run(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    Note:
        For more information on how Flask integrates with Cloud
        Functions, see the `Writing HTTP functions` page.
        <https://cloud.google.com/functions/docs/writing/http#http_frameworks>
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
        return "Provide after arg"

    downloader = qpi_downloader.QPIDownloader(
        TYPEFORM_API_KEY,
        NEW_PROD_FORM_ID,
        answers_file_name,
        questions_file_name,
        after,
    )

    downloader.run()

    return "Done"
