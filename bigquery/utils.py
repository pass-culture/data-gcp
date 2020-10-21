import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def log_query_statistics(nb_rows, query_job):
    elapsed_time = round((query_job.ended - query_job.created).total_seconds(), 2)
    Mb_processed = round(query_job.total_bytes_processed / 1000000, 2)
    logger.info(f"Affected {nb_rows} lines ({Mb_processed} Mb processed) in {elapsed_time} sec")


def run_query(bq_client, query, **kwargs):
    query_job = bq_client.query(query=query, **kwargs)
    logger.info(f"Query running: < {query_job.query} >")
    results = query_job.result()
    log_query_statistics(results.total_rows, query_job)
