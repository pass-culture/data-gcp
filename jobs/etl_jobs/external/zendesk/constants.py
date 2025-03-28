from google.cloud import bigquery

MACRO_ACTIONS_MAPPING = {
    "set_tags": "tags",
    "subject": "subject",
    "comment_value_html": "html",
    "custom_fields_4490222594460": "typology_support_native",
    "custom_fields_4490267156124": "typology_support_pro",
}

MACRO_BASE_COLUMNS = [
    "id",
    "title",
    "created_at",
    "updated_at",
    "url",
    "raw_title",
    "usage_1h",
    "usage_24h",
    "usage_7d",
    "usage_30d",
]

TICKET_CUSTOM_FIELDS = {
    360026372978: "subject",
    360026372998: "description",
    360026373018: "status",
    1900005591693: "issue",
    4490222594460: "typology_support_native",
    4490267156124: "typology_support_pro",
    13689298822684: "typology_fraud_native",
    11756102293788: "technical_partner",
    360029161637: "siren_siret",
    360029405277: "level",
    360026373098: "assignee",
    360026373078: "group",
    360026373058: "priority",
}

TICKET_BASE_COLUMNS = [
    "id",
    "group_id",
    "assignee_id",
    "recipient",
    "brand_id",
    "created_at",
    "updated_at",
    "subject",
    "description",
    "external_id",
    "requester_id",
    "tags",
    "status",
    "forum_topic_id",
    "ticket_form_id",
]

MACRO_ACTIONS_COLUMNS_BQ_SCHEMA_FIELD = [
    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"),
    bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "created_at", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "updated_at", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField("url", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "raw_title", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "usage_1h", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "usage_24h", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "usage_7d", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "usage_30d", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField("tags", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField("html", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "typology_support_native", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "subject", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "typology_support_pro", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "export_date", bigquery.enums.SqlTypeNames.DATE, mode="NULLABLE"
    ),
]


TICKET_COLUMN_BQ_SCHEMA_FIELD = [
    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"),
    bigquery.SchemaField(
        "group_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "assignee_id", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "brand_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField("status", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "recipient", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "created_at", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "updated_at", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "subject", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "description", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "external_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "requester_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField("tags", bigquery.enums.SqlTypeNames.STRING, mode="REPEATED"),
    bigquery.SchemaField(
        "forum_topic_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "ticket_form_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField("issue", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "typology_support_native", bigquery.enums.SqlTypeNames.STRING, mode="REPEATED"
    ),
    bigquery.SchemaField(
        "typology_support_pro", bigquery.enums.SqlTypeNames.STRING, mode="REPEATED"
    ),
    bigquery.SchemaField(
        "technical_partner", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "typology_fraud_native", bigquery.enums.SqlTypeNames.STRING, mode="REPEATED"
    ),
    bigquery.SchemaField(
        "siren_siret", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField("level", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "user_id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "updated_date", bigquery.enums.SqlTypeNames.DATE, mode="NULLABLE"
    ),
]

SURVEY_RESPONSE_COLUMN_BQ_SCHEMA_FIELD = [
    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"),
    bigquery.SchemaField(
        "ticket_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "assignee_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "group_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "requester_id", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField("score", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "comment", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "created_at", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "updated_at", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField("url", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "export_date", bigquery.enums.SqlTypeNames.DATE, mode="NULLABLE"
    ),
]
