import logging
from google.cloud import bigquery


def restore_to_big_query():

    # TO DO : To put in env var
    source_path = "gs://pass-culture-data/csv_dumps_for_bigquery/"
    source_id = "pass-culture-app-projet-test.sandbox_data."

    client = bigquery.Client()

    table_dict = {
        "offer_test_restore": "offer_202010161150.csv",
        "user_test_restore": "_user__202010161150.csv",
        "booking_test_restore": "booking_202010161150.csv",
    }

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition="WRITE_TRUNCATE",
    )

    for table_id, uri_source in table_dict.items():
        logging.info("-------")
        logging.info("Loading table %s.", table_id)
        restore_table(
            source_path + uri_source, source_id + table_id, client, job_config
        )
        logging.info("-------")

    return


def restore_table(uri_source, table_id, client, job_config):

    load_job = client.load_table_from_uri(uri_source, table_id, job_config=job_config)

    load_job.result()

    destination_table = client.get_table(table_id)
    logging.info("Loaded %s rows.", destination_table.num_rows)
