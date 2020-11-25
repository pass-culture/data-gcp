import sys

from google.cloud import bigquery

from analytics.config import BIGQUERY_POC_DATASET
from analytics.utils import run_query
from dependencies.data_analytics.anonymization import (
    anonymize_validation_token_offerer,
    anonymize_apikey,
    anonymize_firstname,
    anonymize_lastname,
    anonymize_dateofbirth,
    anonymize_phonenumber,
    anonymize_email,
    anonymize_publicname,
    anonymize_password,
    anonymize_validation_token_user,
    anonymize_reset_password_token,
    anonymize_iban_bic,
    anonymize_iban_payment,
    anonymize_bic_payment,
    anonymize_validation_token_user_offerer,
    anonymize_token,
    anonymize_validation_token_venue,
)
from set_env import set_env_vars

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def main():
    # run query
    client = bigquery.Client()

    run_query(
        bq_client=client,
        query=anonymize_validation_token_offerer(dataset=BIGQUERY_POC_DATASET),
    )
    run_query(bq_client=client, query=anonymize_apikey(dataset=BIGQUERY_POC_DATASET))
    run_query(bq_client=client, query=anonymize_firstname(dataset=BIGQUERY_POC_DATASET))
    run_query(bq_client=client, query=anonymize_lastname(dataset=BIGQUERY_POC_DATASET))
    run_query(
        bq_client=client, query=anonymize_dateofbirth(dataset=BIGQUERY_POC_DATASET)
    )
    run_query(
        bq_client=client, query=anonymize_phonenumber(dataset=BIGQUERY_POC_DATASET)
    )
    run_query(bq_client=client, query=anonymize_email(dataset=BIGQUERY_POC_DATASET))
    run_query(
        bq_client=client, query=anonymize_publicname(dataset=BIGQUERY_POC_DATASET)
    )
    run_query(bq_client=client, query=anonymize_password(dataset=BIGQUERY_POC_DATASET))
    run_query(
        bq_client=client,
        query=anonymize_validation_token_user(dataset=BIGQUERY_POC_DATASET),
    )
    run_query(
        bq_client=client,
        query=anonymize_reset_password_token(dataset=BIGQUERY_POC_DATASET),
    )
    run_query(bq_client=client, query=anonymize_iban_bic(dataset=BIGQUERY_POC_DATASET))
    run_query(
        bq_client=client, query=anonymize_iban_payment(dataset=BIGQUERY_POC_DATASET)
    )
    run_query(
        bq_client=client, query=anonymize_bic_payment(dataset=BIGQUERY_POC_DATASET)
    )
    run_query(
        bq_client=client,
        query=anonymize_validation_token_user_offerer(dataset=BIGQUERY_POC_DATASET),
    )
    run_query(bq_client=client, query=anonymize_token(dataset=BIGQUERY_POC_DATASET))
    run_query(
        bq_client=client,
        query=anonymize_validation_token_venue(dataset=BIGQUERY_POC_DATASET),
    )


if __name__ == "__main__":
    set_env_vars()
    main()
