import io
import imagehash
import mFiles
import pandas as pd
from PIL import Image

from utils import BIGQUERY_ANALYTICS_DATASET, GCP_PROJECT


hashfunc = lambda img: imagehash.phash(img, hash_size=16, highfreq_factor=10)


def run(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and "application_id" in request.args:
        application_id = request.args.get("application_id")
    elif request_json and "application_id" in request_json:
        application_id = request_json["application_id"]
    else:
        raise RuntimeError("You need to provide an `application_id` argument.")

    record = process_frame(application_id)
    save_to_bq(record)

    return record[0].to_db()


def process_frame(application_id):
    try:
        (
            img,
            idx,
            details,
        ) = mFiles.get_application_image(application_id)
        hash_img = hashfunc(Image.open(io.BytesIO(img)))
    except Exception as e:
        raise RuntimeError("Problem:", e, "with", application_id)

    record = (hash_img, details)

    return record


def save_to_bq(record):
    (hash_img, details) = record
    columns = ["applicationId", "hash_img", "objectId", "fileId"]

    df = pd.DataFrame(
        [
            [
                f'{details["applicationId"]}',
                hash_img.to_db(),
                f'{details["objectId"]}',
                f'{details["fileId"]}',
            ]
        ],
        columns=columns,
    )
    print(df.dtypes)
    df.to_gbq(
        f"{BIGQUERY_ANALYTICS_DATASET}.hashed-cni",
        project_id=GCP_PROJECT,
        if_exists="append",
    )
