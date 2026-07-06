import logging
import time

import requests
import typer

from constants import (
    API_URL,
    DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME,
    DMS_TOKEN,
    GCP_PROJECT_ID,
    demarches,
)
from dms_query_w_champs import DMS_QUERY as DMS_QUERY
from dms_query_wo_champs import DMS_QUERY as DMS_QUERY_REDUCED
from utils import mergeDictionary, save_json

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def run(target: str, updated_since: str):
    logger.info(f"updated_since={updated_since}")

    if target not in ("jeunes", "pro"):
        logger.error(f"Unknown target: {target}")
        raise typer.Exit(code=1)

    try:
        fetch_dms(updated_since, demarches=demarches[target], target=target)
    except Exception as e:
        logger.error(f"Failed to fetch DMS for target={target}: {e}")
        raise typer.Exit(code=1)


def fetch_dms(updated_since, demarches, target):
    result = fetch_result(demarches, updated_since)
    logger.info(f"Fetched {len(result['data'])} demarches for target={target}")
    save_json(
        result,
        f"gs://{DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME}/dms_export/unsorted_dms_{target}_{updated_since}.json",
        GCP_PROJECT_ID,
    )


def fetch_result(demarches_ids, updated_since):
    result = {}
    for demarche_id in demarches_ids:
        logger.info(f"Fetching demarche {demarche_id}")
        if demarche_id in demarches["reduced"]:
            dms_query = DMS_QUERY_REDUCED
            logger.info("dms query: reduced")
        else:
            dms_query = DMS_QUERY
            logger.info("dms query: default")
        end_cursor = ""
        query_body = get_query_body(demarche_id, dms_query, "", updated_since)
        has_next_page = True
        while has_next_page:
            logger.info("Fetching next page..")
            has_next_page = False
            resultTemp = run_query(query_body)
            if "errors" in resultTemp:
                logger.warning(f"GraphQL errors: {resultTemp['errors']}")
            if resultTemp["data"] is not None:
                for node in resultTemp["data"]["demarche"]["dossiers"]["edges"]:
                    dossier = node["node"]
                    if dossier is not None:
                        dossier["demarche_id"] = demarche_id
                result = mergeDictionary(result, resultTemp)

                has_next_page = resultTemp["data"]["demarche"]["dossiers"]["pageInfo"][
                    "hasNextPage"
                ]
                if has_next_page:
                    end_cursor = resultTemp["data"]["demarche"]["dossiers"]["pageInfo"][
                        "endCursor"
                    ]
                    query_body = get_query_body(
                        demarche_id, dms_query, end_cursor, updated_since
                    )

    if not isinstance(result["data"], list):
        result["data"] = [result["data"]]
    return result


def get_query_body(demarche_id, dms_query, end_cursor, updated_since):
    variables = {
        "demarcheNumber": demarche_id,
        "after": end_cursor,
        "updatedSince": updated_since,
    }
    query_body = {"query": dms_query, "variables": variables}
    return query_body


def run_query(query_body):
    time.sleep(0.2)

    headers = {
        "Authorization": f"Bearer {DMS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    response = requests.post(
        API_URL, json=query_body, headers=headers, verify=True, timeout=600
    )
    if response.status_code == 200:
        return response.json()
    else:
        raise RuntimeError(
            f"Query failed with status {response.status_code}: {response.text}"
        )


if __name__ == "__main__":
    logger.info("Run DMS !")
    app()
