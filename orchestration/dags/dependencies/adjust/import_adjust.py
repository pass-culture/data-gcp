SOURCE_PROJECT = "passculture-data-ehp"
SOURCE_DATASET = "adjust_import_dev"
SOURCE_LOCATION = "EU"
COPY_DATASET = "tmp_dev"
PUBLISH_DATASET = "raw_dev"
DESTINATION_LOCATION = "europe-west1"
EXPECTED_SCHEMA = {
    "app_token": "STRING",
    "os_name": "STRING",
    "day": "DATETIME",
    "channel": "STRING",
    "campaign_network": "STRING",
    "adgroup_network": "STRING",
    "cost": "FLOAT64",
    "attribution_impressions": "INT64",
    "attribution_clicks": "INT64",
    "installs": "INT64",
    "skad_direct_installs": "INT64",
    "registration_m36_events_cohort": "INT64",
    "registration_18_m36_events_cohort": "INT64",
    "underage_registration_m36_events_cohort": "INT64",
    "complete_beneficiary_m36_events_cohort": "INT64",
    "complete_beneficiary_18_m36_events_cohort": "INT64",
    "complete_beneficiary_underage_m36_events_cohort": "INT64",
    "complete_beneficiary_m36_conversions_cohort": "INT64",
}
