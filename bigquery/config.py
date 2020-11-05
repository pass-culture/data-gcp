# GCP configs
GCP_PROJECT_ID = "pass-culture-app-projet-test"
GCP_REGION = "europe-west1"
CLOUDSQL_DATABASE = "cloud_SQL_dump-prod-8-10-2020"
BIGQUERY_POC_DATASET = "poc_data_federated_query"
BASE32_JS_LIB_PATH = "gs://pass-culture-data/base32-encode/base32.js"

# Data
OFFER_TABLE_NAME = "offer"
OFFER_ID = "id"
OFFER_COLUMNS = [
    "idAtProviders",
    "dateModifiedAtLastProvider",
    "id",
    "dateCreated",
    "productId",
    "venueId",
    "lastProviderId",
    "bookingEmail",
    "isActive",
    "type",
    "name",
    "description",
    "conditions",
    "ageMin",
    "ageMax",
    "url",
    "mediaUrls",
    "durationMinutes",
    "isNational",
    "extraData",
    "isDuo",
    "fieldsUpdated",
    "withdrawalDetails",
]

# Enriched Data
MIGRATION_ENRICHED_VENUE_DATA = "migration_enriched_venue_data"
MIGRATION_ENRICHED_OFFERER_DATA = "migration_enriched_offerer_data"
