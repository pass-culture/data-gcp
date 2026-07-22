import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "stg")

H3_RESOLUTION = 9


# External POI data : base_lieux_culturels_ouverts
POI_SOURCE = "base_lieux_culturels_ouverts"
POI_TABLE = f"raw_{ENV_SHORT_NAME}.{POI_SOURCE}"

POI_CSV_ID_COL = "id"
POI_ID_COL = "poi_id"
POI_NAME_COL = "nom_du_lieu"
POI_ADDRESS_COL = "adresse_complete"
POI_POSTAL_CODE_COL = "code_postal"
POI_COMMUNE_COL = "commune"
POI_LATITUDE_COL = "latitude"
POI_LONGITUDE_COL = "longitude"
POI_H3_INDEX_COL = "h3_index"

# Applicative table: offerer_address
APPLICATIVE_SOURCE = "offerer_address"
OFFERER_ADDRESS_TABLE = f"int_applicative_{ENV_SHORT_NAME}.{APPLICATIVE_SOURCE}"

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
