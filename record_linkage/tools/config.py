import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")

MAX_OFFER_PER_BATCH = 50000
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

data_and_hyperparams_dict = {
    "performer": {
        "features": {
            "offer_name": {"method": "jarowinkler", "threshold": 0.95},
            "offer_description": {"method": "jarowinkler", "threshold": 0.95},
            "performer": {"method": "exact"},
        },
        "matches_required": 2,
    },
    "non_performer": {
        "features": {
            "offer_name": {"method": "jarowinkler", "threshold": 0.95},
            "offer_description": {"method": "jarowinkler", "threshold": 0.95},
        },
        "matches_required": 1,
    },
}
