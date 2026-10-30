import os

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-prod")

ML_INPUT_DATASET = f"ml_input_{ENV_SHORT_NAME}"
EMBEDDING_DATASET = f"ml_semantic_embedding_{ENV_SHORT_NAME}"

GTL_LEVELS = [
    "gtl_label_level_1",
    "gtl_label_level_2",
    "gtl_label_level_3",
    "gtl_label_level_4",
]

# Columns added to the cached item table by the `lemmatize` command (see
# src/lemmatization.py), and left out when loading it for sampling: the
# sample gets its own `entities` from `preprocess`.
NOUN_LEMMAS_COL = "noun_lemmas"
VERB_LEMMAS_COL = "verb_lemmas"
LEMMA_COLS = (NOUN_LEMMAS_COL, VERB_LEMMAS_COL)
ENTITIES_COL = "entities"
FULL_TABLE_NLP_COLS = (*LEMMA_COLS, ENTITIES_COL)
