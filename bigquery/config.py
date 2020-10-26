# GCP configs
GCP_PROJECT_ID = "pass-culture-app-projet-test"
GCP_REGION = "europe-west1"
CLOUDSQL_DATABASE = "cloud_SQL_dump-prod-8-10-2020"
BIGQUERY_POC_DATASET = "poc_data_federated_query"

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
