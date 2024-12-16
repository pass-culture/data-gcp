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
    "brand_id",
    "created_at",
    "subject",
    "description",
    "external_id",
    "requester_id",
    "tags",
    "forum_topic_id",
    "ticket_form_id",
]
