# GCP configs
GCP_PROJECT_ID = "passculture-data-ehp"
GCP_REGION = "europe-west1"
CLOUDSQL_DATABASE = "cloud_SQL_dump-prod-8-10-2020"
BIGQUERY_POC_DATASET = "poc_data_federated_query"
TABLE_PREFIX = ""
BASE32_JS_LIB_PATH = "gs://pass-culture-data/base32-encode/base32.js"

# Data
OFFER_TABLE_NAME = "offer"
OFFER_ID = "id"
OFFER_COLUMNS = [
    "idAtProvider",
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

# Anonymization
ANONYMIZATION_TABLES = [
    "user",
    "provider",
    "offerer",
    "bank_information",
    "booking",
    "payment",
    "venue",
    "user_offerer",
]

# Enriched Data
MIGRATION_ENRICHED_VENUE_DATA = "migration_enriched_venue_data"
MIGRATION_ENRICHED_OFFERER_DATA = "migration_enriched_offerer_data"
MIGRATION_ENRICHED_USER_DATA = "migration_enriched_user_data"
MIGRATION_ENRICHED_OFFER_DATA = "migration_enriched_offer_data"
MIGRATION_ENRICHED_STOCK_DATA = "migration_enriched_stock_data"
MIGRATION_ENRICHED_BOOKING_DATA = "migration_enriched_booking_data"
ENRICHED_OFFERER_DATA_TABLES = ["offerer", "venue", "offer", "stock", "booking"]
ENRICHED_OFFER_DATA_TABLES = [
    "offer",
    "stock",
    "booking",
    "favorite",
    "venue",
    "offerer",
]
ENRICHED_USER_DATA_TABLES = ["booking", "stock", "offer", "user"]
ENRICHED_VENUE_DATA_TABLES = [
    "offerer",
    "venue",
    "offer",
    "stock",
    "booking",
    "venue_type",
    "venue_label",
]
ENRICHED_STOCK_DATA_TABLES = [
    "stock",
    "booking",
    "payment",
    "payment_status",
    "offer",
    "venue",
]
ENRICHED_BOOKING_DATA_TABLES = [
    "booking",
    "payment",
    "payment_status",
    "stock",
    "offer",
    "offerer",
    "venue",
    "user",
    "venue_type",
    "venue_label",
]
