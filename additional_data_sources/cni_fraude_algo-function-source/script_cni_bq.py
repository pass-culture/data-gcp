from google.cloud import bigquery
from tqdm import tqdm

client = bigquery.Client()

TABLE = "passculture-data-prod.analytics_prod.algo-cni-results"


for k in tqdm(range(0, 146 * 5)):

    OFFSET = 15000 + k * 1000
    job_config = bigquery.QueryJobConfig(
        destination=TABLE,
        write_disposition="WRITE_APPEND",
        query_parameters=[
            bigquery.ScalarQueryParameter("offset", "INT64", OFFSET),
        ],
    )

    QUERY = """with candidate as (
            SELECT hash_img, applicationId from `passculture-data-prod.analytics_prod.hashed-cni`
            order by applicationId
            LIMIT 1000 OFFSET @offset
        ),
        couples AS (
            SELECT ha.hash_img as hash_img1, ha.applicationId as applicationId_1,
            candidate.hash_img as hash_img2, candidate.applicationId as applicationId_2,
            FROM candidate, `passculture-data-prod.analytics_prod.hashed-cni` ha
            WHERE ha.applicationId < candidate.applicationId
            AND ha.applicationId > candidate.applicationId - 50000
        ),
        distances AS (
            SELECT applicationId_1, applicationId_2,
            analytics_prod.levenshtein(hash_img1, hash_img2) distance,
            FROM couples
        )
        SELECT * FROM distances
        WHERE distance < 18
        """

    query_job = client.query(QUERY, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

print("Done")
