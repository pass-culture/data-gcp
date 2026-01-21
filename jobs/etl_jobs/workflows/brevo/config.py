import os

GCP_PROJECT = os.environ.get("GCP_PROJECT")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_TMP_DATASET = f"tmp_{ENV_SHORT_NAME}"

TRANSACTIONAL_TABLE_NAME = "brevo_transactional_detailed"
UPDATE_WINDOW = 31 if ENV_SHORT_NAME == "prod" else 500


def get_api_configuration(audience: str):
    """
    Returns the NAME of the secret and the table, not the values.
    """
    if audience == "native":
        # This is the NAME in Google Secret Manager
        secret_id = f"sendinblue-api-key-{ENV_SHORT_NAME}"
        table_name = "brevo_newsletters"
    elif audience == "pro":
        secret_id = f"sendinblue-pro-api-key-{ENV_SHORT_NAME}"
        table_name = "brevo_pro_newsletters"
    else:
        raise ValueError(f"Invalid audience: {audience}")

    return secret_id, table_name
