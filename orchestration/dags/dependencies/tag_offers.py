import pandas as pd
import pandas_gbq as gbq
from dependencies.bigquery_client import BigQueryClient
from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    DATA_GCS_BUCKET_NAME,
)
from dependencies.Offer_name_tags import (
    extract_tags_offer_name,
)

TagDict = {
    "INSTRUMENT": [
        "guitare",
        "manche",
        "touche",
        "cordes",
        "acajou",
        "chevalet",
        "clavier",
        "musique",
        "usb",
        "housse",
        "micro",
        "jack",
        "sillet",
        "micros",
        "éclisses",
        "palissandre",
        "touches",
        "piano",
        "casque",
        "ukulélé",
    ],
    "MUSEE": [
        "exposition",
        "musée",
        "visite",
        "découvrir",
        "art",
        "histoire",
        "siècle",
        "visites",
        "château",
        "patrimoine",
    ],
    "SPECTACLE": [
        "spectacle",
        "histoire",
        "théâtre",
        "danse",
        "amour",
        "musique",
        "humour",
        "pièce",
        "création",
        "festival",
        "compagnie",
        "danseurs",
        "cirque",
        "rire",
        "famille",
    ],
    "MUSIQUE_ENREGISTREE": [
        "album",
        "musique",
        "vinyle",
        "groupe",
        "rock",
        "concert",
        "pop",
        "rap",
        "festival",
        "voix",
        "nouveau",
        "jazz",
    ],
    "PRATIQUE_ART": [
        "cours",
        "danse",
        "atelier",
        "musique",
        "pratique",
        "guitare",
        "découvrir",
        "piano",
        "ateliers",
        "stage",
    ],
}

TAG_OFFERS_CATEGORIES = [categories for categories in TagDict.keys()]
OFFERS_TO_TAG_MAX_LENGTH = 1000
FILENAME_INITIAL = f"{DATA_GCS_BUCKET_NAME}/offer_tags/offers_to_tag.csv"
FILENAME_DESCRIPTION = f"{DATA_GCS_BUCKET_NAME}/offer_tags/tag_description.csv"
FILENAME_OFFER_NAME = f"{DATA_GCS_BUCKET_NAME}/offer_tags/tag_offer_name.csv"


def merge_dataframes(df1, df2):
    return pd.concat([df1, df2], ignore_index=True)


def save_to_csv(dataframe, filename):
    dataframe.to_csv(f"gs://{filename}")
    return


def load_from_csv(filename):
    return pd.read_csv(f"gs://{filename}")


def get_offers_to_tag_request():
    return f"""WITH offers_CatAgg AS (
            SELECT
                offer.offer_id as offer_id,
                offer.offer_name as offer_name,
                offer.offer_description as description,
                offer.offer_subcategoryId as offer_subcategoryId,
                subcategories.category_id as categorie_principale
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offer` offer
            JOIN `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.subcategories` subcategories ON offer.offer_subcategoryId = subcategories.id
            )
            SELECT offer_id, offer_name, categorie_principale, description FROM offers_CatAgg
            WHERE description <> 'none'
            AND   description <> ""
            AND   offer_id NOT In (SELECT offer_id FROM {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.offer_tags)
            LIMIT 10000
            """


def fetch_offers_to_tag():
    return pd.read_gbq(
        f""" SELECT * FROM {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.temp_offers_to_tag"""
    )


def tag_descriptions(offers_to_tag, TopicList):
    offer_tagged = []
    for index, row in offers_to_tag.iterrows():
        descrip_dict = {"offer_id": f"""{row["offer_id"]}"""}
        description_topic = []
        for word in TopicList:
            if word in row["description"].lower():
                description_topic.append(word)
        if len(description_topic) > 0:
            descrip_dict["tag"] = description_topic
            offer_tagged.append(descrip_dict)

    return pd.DataFrame(offer_tagged)


def tag_offers_description():
    # offers_to_tag = load_from_csv(FILENAME_INITIAL)
    offers_to_tag = fetch_offers_to_tag()
    df_offers_tagged_list = []
    for category in TAG_OFFERS_CATEGORIES:
        df_offers_tagged_list.append(
            tag_descriptions(
                offers_to_tag[offers_to_tag["categorie_principale"] == f"{category}"],
                TagDict[f"{category}"],
            )
        )
    offers_description_tagged = pd.concat(df_offers_tagged_list)
    save_to_csv(offers_description_tagged, FILENAME_DESCRIPTION)
    return


def tag_offers_name():
    save_to_csv(extract_tags_offer_name(fetch_offers_to_tag()), FILENAME_OFFER_NAME)
    return


def prepare_table():
    # dfinit table with all offers to tag not present in offer_tags
    df_offers_to_tag = fetch_offers_to_tag()
    df_description_tags = load_from_csv(FILENAME_DESCRIPTION)
    df_offer_name_tags = load_from_csv(FILENAME_OFFER_NAME)

    df_offers_to_tag = df_offers_to_tag.astype({"offer_id": "string"})
    df_description_tags = df_description_tags.astype({"offer_id": "string"})
    df_offer_name_tags = df_offer_name_tags.astype({"offer_id": "string"})

    # df12 merge of offer_name and description
    df_all_tags = merge_dataframes(df_description_tags, df_offer_name_tags)
    df_all_tags = df_all_tags.astype({"offer_id": "string"})

    # df3 offer wO tags
    df_offers_wo_tags = df_offers_to_tag[
        ~df_offers_to_tag.offer_id.isin(df_all_tags.offer_id)
    ].assign(tag="none")
    # df_final , should be the same as dfinit but all the offers have a tag or 'none'
    df_offers_tagged = merge_dataframes(df_all_tags, df_offers_wo_tags)
    df_offers_tagged["offer_id"] = df_offers_tagged["offer_id"].apply(str)
    df_offers_tagged[["offer_id", "tag"]].to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.temp_offer_tagged""",
        project_id=f"{GCP_PROJECT}",
        if_exists="replace",
    )
    return


def get_upsert_request():
    return f"""
        MERGE `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.offer_tags` T
        USING `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.temp_offer_tagged` S
        ON T.offer_id = S.offer_id
        WHEN MATCHED THEN
            UPDATE SET tag = s.tag
        WHEN NOT MATCHED THEN
            INSERT (offer_id, tag) VALUES(offer_id, tag)
        """
