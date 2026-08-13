import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "stg")

H3_RESOLUTION = 9
SEARCH_RADIUS_METERS = 500

# External POI data : base_lieux_culturels_ouverts
POI_SOURCE_TABLE = "base_lieux_culturels_ouverts"
POI_TABLE = f"raw_{ENV_SHORT_NAME}.{POI_SOURCE_TABLE}"

POI_CSV_ID_COL = "id"
POI_ID_COL = "poi_id"
POI_NAME_COL = "nom_du_lieu"
POI_COMMON_NAME_COL = "nom_usuel_du_lieu"
POI_ADDRESS_COL = "adresse_complete"
POI_PUBLIC_ENTRANCE_ADDRESS_COL = "adresse_entree_public"
POI_POSTAL_CODE_COL = "code_postal"
POI_COMMUNE_COL = "commune"
POI_LATITUDE_COL = "latitude"
POI_LONGITUDE_COL = "longitude"
POI_H3_INDEX_COL = "h3_index"
POI_SIRET_COL = "siret"

# Applicative table: offerer_address
APPLICATIVE_SOURCE_TABLE = "offerer_address"
OFFERER_ADDRESS_TABLE = f"int_applicative_{ENV_SHORT_NAME}.{APPLICATIVE_SOURCE_TABLE}"

OFFERER_ADDRESS_ID_COL = "offerer_address_id"
OFFERER_ADDRESS_LABEL_COL = "offerer_address_label"
OFFERER_ADDRESS_TYPE_COL = "offerer_address_type"
OFFERER_ID_COL = "offerer_id"
ADDRESS_ID_COL = "address_id"
ADDRESS_STREET_COL = "address_street"
ADDRESS_POSTAL_CODE_COL = "address_postal_code"
ADDRESS_CITY_COL = "address_city"
ADDRESS_DEPARTMENT_CODE_COL = "address_department_code"
ADDRESS_LATITUDE_COL = "address_latitude"
ADDRESS_LONGITUDE_COL = "address_longitude"
VENUE_ID_FK_COL = "venue_id"
ADDRESS_H3_INDEX_COL = "h3_index"

# Applicative table: offerer
OFFERER_SOURCE_TABLE = "offerer"
OFFERER_TABLE = f"int_applicative_{ENV_SHORT_NAME}.{OFFERER_SOURCE_TABLE}"
OFFERER_SIREN_COL = "offerer_siren"

# Sandbox: offerer_address_poi_link (see src.matching.build_offerer_address_poi_link)
SANDBOX_SCHEMA = f"sandbox_{ENV_SHORT_NAME}"
OFFERER_ADDRESS_POI_LINK_SOURCE_TABLE = "offerer_address_poi_link"
OFFERER_ADDRESS_POI_LINK_TABLE = f"{SANDBOX_SCHEMA}.{OFFERER_ADDRESS_POI_LINK_SOURCE_TABLE}"

# Candidate search
DISTANCE_METERS_COL = "distance_meters"
BATCH_SIZE = 50000
