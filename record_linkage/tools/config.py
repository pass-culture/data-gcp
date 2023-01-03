import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")

SUBCATEGORIES_WITH_PERFORMER = [
    "CONCERT",
    "FESTIVAL_MUSIQUE",
    "EVENEMENT_MUSIQUE",
    "FESTIVAL_SPECTACLE",
    "SPECTACLE_ENREGISTRE",
    "TELECHARGEMENT_MUSIQUE",
    "SPECTACLE_REPRESENTATION",
    "SPECTACLE_VENTE_DISTANCE",
    "SUPPORT_PHYSIQUE_MUSIQUE",
]
SUBSET_MAX_LENGTH = 1000
