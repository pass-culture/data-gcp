import io
import imagehash
import mFiles
import pandas as pd
from PIL import Image
from google.cloud import bigquery

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

    print("Starting...")
    client = bigquery.Client()

    application_id_min = application_id - 500

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "application_id_min", "INT64", application_id_min
            ),
            bigquery.ScalarQueryParameter(
                "application_id_max", "INT64", application_id
            ),
        ],
    )
    query = f"""
        SELECT applicationId
        FROM `{BIGQUERY_ANALYTICS_DATASET}.hashed-cni`
        WHERE applicationId > @application_id_min
        AND applicationId <= @application_id_max
    """

    query_job = client.query(query, job_config=job_config)
    applications_seen = query_job.result().to_dataframe().values

    applications = []

    print(f"{len(applications_seen)} applications already seen !")
    print("Hashing images...")
    for application in range(application_id_min + 1, application_id + 1):
        if application % 100 == 0:
            print(application, "/", application_id)
        if application not in applications_seen:
            (hash_img, details) = process_frame(application)
            if hash_img is not None:
                applications.append(
                    [
                        details["applicationId"],
                        hash_img.to_db(),
                        f'{details["objectId"]}',
                        f'{details["fileId"]}',
                    ]
                )

    columns = ["applicationId", "hash_img", "objectId", "fileId"]

    applications_df = pd.DataFrame(
        applications,
        columns=columns,
    )

    print(f"Hashed {applications_df.shape[0]} cni.")

    if applications_df.shape[0] == 0:
        print(f"No new hashed cni, skipping save to BQ...")
    else:
        # Save to BQ
        applications_df.to_gbq(
            f"{BIGQUERY_ANALYTICS_DATASET}.hashed-cni",
            project_id=GCP_PROJECT,
            if_exists="append",
        )
        print(f"Added {applications_df.shape[0]} hashed cni to BQ.")

    print("Detecting frauds...")
    compare_with_others(application_id)
    print("Done")
    return "Success"


def process_frame(application_id):
    try:
        (
            img,
            idx,
            details,
        ) = mFiles.get_application_image(application_id)
        hash_img = hashfunc(Image.open(io.BytesIO(img)))
        record = (hash_img, details)
    except Exception as e:
        print("Problem:", e, "with", application_id)
        record = (None, None)

    return record


def compare_with_others(application_id):
    client = bigquery.Client()

    table = f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.algo-cni-results"

    job_config = bigquery.QueryJobConfig(
        destination=table,
        write_disposition="WRITE_APPEND",
        query_parameters=[
            bigquery.ScalarQueryParameter("application_id", "INT64", application_id),
        ],
    )

    QUERY = f"""with candidate as (
            SELECT hash_img, applicationId FROM `{BIGQUERY_ANALYTICS_DATASET}.hashed-cni`
            WHERE applicationId <= @application_id
            AND applicationId > @application_id - 500
            AND applicationId not in (SELECT applicationId_2 FROM `{BIGQUERY_ANALYTICS_DATASET}.algo-cni-results`)
        ), 
        distances AS (
            SELECT ha.applicationId as applicationId_1, candidate.applicationId as applicationId_2,
            {BIGQUERY_ANALYTICS_DATASET}.levenshtein(ha.hash_img, candidate.hash_img) distance,
            FROM candidate, `{BIGQUERY_ANALYTICS_DATASET}.hashed-cni` ha
            WHERE ha.applicationId < candidate.applicationId
            AND ha.applicationId > candidate.applicationId - 50000
        )
        SELECT * FROM distances
        WHERE distance < 18
        """

    query_job = client.query(QUERY, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.
