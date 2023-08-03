import numpy as np
import os
import json


ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
CONFIGS_PATH = os.environ.get("CONFIGS_PATH", "configs")


##Clusterisation Config
clusterisation_reduction = 2
clusterisation_reduct_method = "umap"

categories_config = [
    {
        "group": "MANGA",
        "features": [
            {"type": "offer_type_label", "name": "Manga"},
        ],
    },
    {
        "group": "LIVRES",
        "features": [
            {"type": "category", "name": "LIVRE"},
            {"type": "category", "name": "MEDIA"},
        ],
    },
    {
        "group": "CINEMA",
        "features": [
            {"type": "category", "name": "FILM"},
            {"type": "category", "name": "CINEMA"},
        ],
    },
    {
        "group": "MUSIQUE",
        "features": [
            {"type": "category", "name": "MUSIQUE_ENREGISTREE"},
            {"type": "category", "name": "MUSIQUE_LIVE"},
            {"type": "category", "name": "INSTRUMENT"},
        ],
    },
    {
        "group": "ARTS",
        "features": [
            {"type": "category", "name": "MUSEE"},
            {"type": "category", "name": "CARTE_JEUNES"},
            {"type": "category", "name": "BEAUX_ARTS"},
            {"type": "category", "name": "PRATIQUE_ART"},
        ],
    },
    {
        "group": "SORTIES",
        "features": [
            {"type": "category", "name": "SPECTACLE"},
            {"type": "category", "name": "JEU"},
            {"type": "category", "name": "CONFERENCE"},
        ],
    },
]


def convert_str_emb_to_float(emb_list, emb_size=5):
    float_emb = []
    for str_emb in emb_list:
        emb = json.loads(str_emb)
        float_emb.append(np.array(emb))
    return float_emb
