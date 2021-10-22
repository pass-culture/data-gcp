import pandas as pd
import pandas_gbq as gbq
from dependencies.bigquery_client import BigQueryClient
from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_CLEAN_DATASET,
    DATA_GCS_BUCKET_NAME,
)
from dependencies.Offer_name_tags import (
    extract_tags_offer_name,
)

CaseCatAgg = """CASE
                when offer.offer_subcategoryId = 'SUPPORT_PHYSIQUE_FILM' then 'FILM' 
                when offer.offer_subcategoryId = 'ABO_MEDIATHEQUE' then 'FILM' 
                when offer.offer_subcategoryId = 'VOD' then 'FILM' 
                when offer.offer_subcategoryId = 'ABO_PLATEFORME_VIDEO' then 'FILM' 
                when offer.offer_subcategoryId = 'AUTRE_SUPPORT_NUMERIQUE' then 'FILM' 
                when offer.offer_subcategoryId = 'CARTE_CINE_MULTISEANCES' then 'CINEMA' 
                when offer.offer_subcategoryId = 'CARTE_CINE_ILLIMITE' then 'CINEMA' 
                when offer.offer_subcategoryId = 'SEANCE_CINE' then 'CINEMA' 
                when offer.offer_subcategoryId = 'EVENEMENT_CINE' then 'CINEMA' 
                when offer.offer_subcategoryId = 'FESTIVAL_CINE' then 'CINEMA' 
                when offer.offer_subcategoryId = 'CINE_VENTE_DISTANCE' then 'CINEMA' 
                when offer.offer_subcategoryId = 'CINE_PLEIN_AIR' then 'CINEMA' 
                when offer.offer_subcategoryId = 'CONFERENCE' then 'CONFERENCE_RENCONTRE' 
                when offer.offer_subcategoryId = 'RENCONTRE' then 'CONFERENCE_RENCONTRE' 
                when offer.offer_subcategoryId = 'DECOUVERTE_METIERS' then 'CONFERENCE_RENCONTRE' 
                when offer.offer_subcategoryId = 'SALON' then 'CONFERENCE_RENCONTRE' 
                when offer.offer_subcategoryId = 'CONCOURS' then 'JEU' 
                when offer.offer_subcategoryId = 'RENCONTRE_JEU' then 'JEU' 
                when offer.offer_subcategoryId = 'ESCAPE_GAME' then 'JEU' 
                when offer.offer_subcategoryId = 'EVENEMENT_JEU' then 'JEU' 
                when offer.offer_subcategoryId = 'JEU_EN_LIGNE' then 'JEU' 
                when offer.offer_subcategoryId = 'ABO_JEU_VIDEO' then 'JEU' 
                when offer.offer_subcategoryId = 'ABO_LUDOTHEQUE' then 'JEU' 
                when offer.offer_subcategoryId = 'LIVRE_PAPIER' then 'LIVRE' 
                when offer.offer_subcategoryId = 'LIVRE_NUMERIQUE' then 'LIVRE' 
                when offer.offer_subcategoryId = 'TELECHARGEMENT_LIVRE_AUDIO' then 'LIVRE' 
                when offer.offer_subcategoryId = 'LIVRE_AUDIO_PHYSIQUE' then 'LIVRE' 
                when offer.offer_subcategoryId = 'ABO_BIBLIOTHEQUE' then 'LIVRE' 
                when offer.offer_subcategoryId = 'ABO_LIVRE_NUMERIQUE' then 'LIVRE' 
                when offer.offer_subcategoryId = 'FESTIVAL_LIVRE' then 'LIVRE' 
                when offer.offer_subcategoryId = 'CARTE_MUSEE' then 'MUSEE' 
                when offer.offer_subcategoryId = 'ABO_MUSEE' then 'MUSEE' 
                when offer.offer_subcategoryId = 'VISITE' then 'MUSEE' 
                when offer.offer_subcategoryId = 'VISITE_GUIDEE' then 'MUSEE' 
                when offer.offer_subcategoryId = 'EVENEMENT_PATRIMOINE' then 'MUSEE' 
                when offer.offer_subcategoryId = 'VISITE_VIRTUELLE' then 'MUSEE' 
                when offer.offer_subcategoryId = 'MUSEE_VENTE_DISTANCE' then 'MUSEE' 
                when offer.offer_subcategoryId = 'CONCERT' then 'MUSIQUE_LIVE' 
                when offer.offer_subcategoryId = 'EVENEMENT_MUSIQUE' then 'MUSIQUE_LIVE' 
                when offer.offer_subcategoryId = 'LIVESTREAM_MUSIQUE' then 'MUSIQUE_LIVE' 
                when offer.offer_subcategoryId = 'ABO_CONCERT' then 'MUSIQUE_LIVE' 
                when offer.offer_subcategoryId = 'FESTIVAL_MUSIQUE' then 'MUSIQUE_LIVE' 
                when offer.offer_subcategoryId = 'SUPPORT_PHYSIQUE_MUSIQUE' then 'MUSIQUE_ENREGISTREE' 
                when offer.offer_subcategoryId = 'TELECHARGEMENT_MUSIQUE' then 'MUSIQUE_ENREGISTREE' 
                when offer.offer_subcategoryId = 'ABO_PLATEFORME_MUSIQUE' then 'MUSIQUE_ENREGISTREE' 
                when offer.offer_subcategoryId = 'CAPTATION_MUSIQUE' then 'MUSIQUE_ENREGISTREE' 
                when offer.offer_subcategoryId = 'SEANCE_ESSAI_PRATIQUE_ART' then 'PRATIQUE_ART' 
                when offer.offer_subcategoryId = 'ATELIER_PRATIQUE_ART' then 'PRATIQUE_ART' 
                when offer.offer_subcategoryId = 'ABO_PRATIQUE_ART' then 'PRATIQUE_ART' 
                when offer.offer_subcategoryId = 'ABO_PRESSE_EN_LIGNE' then 'MEDIA' 
                when offer.offer_subcategoryId = 'PODCAST' then 'MEDIA' 
                when offer.offer_subcategoryId = 'APP_CULTURELLE' then 'MEDIA' 
                when offer.offer_subcategoryId = 'SPECTACLE_REPRESENTATION' then 'SPECTACLE' 
                when offer.offer_subcategoryId = 'SPECTACLE_ENREGISTRE' then 'SPECTACLE' 
                when offer.offer_subcategoryId = 'LIVESTREAM_EVENEMENT' then 'SPECTACLE' 
                when offer.offer_subcategoryId = 'FESTIVAL_SPECTACLE' then 'SPECTACLE' 
                when offer.offer_subcategoryId = 'ABO_SPECTACLE' then 'SPECTACLE' 
                when offer.offer_subcategoryId = 'ACHAT_INSTRUMENT' then 'INSTRUMENT' 
                when offer.offer_subcategoryId = 'BON_ACHAT_INSTRUMENT' then 'INSTRUMENT' 
                when offer.offer_subcategoryId = 'LOCATION_INSTRUMENT' then 'INSTRUMENT' 
                when offer.offer_subcategoryId = 'PARTITION' then 'INSTRUMENT' 
                when offer.offer_subcategoryId = 'MATERIEL_ART_CREATIF' then 'BEAUX_ARTS' 
                when offer.offer_subcategoryId = 'ACTIVATION_EVENT' then 'TECHNIQUE' 
                when offer.offer_subcategoryId = 'ACTIVATION_THING' then 'TECHNIQUE' 
                when offer.offer_subcategoryId = 'JEU_SUPPORT_PHYSIQUE' then 'TECHNIQUE' 
                when offer.offer_subcategoryId = 'OEUVRE_ART' then 'TECHNIQUE' 
                else 'Autre'
            END as categorie_principale """

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


def get_offers_to_tag_request():
    return f"""WITH offers_CatAgg AS (
            SELECT offer.offer_id as offer_id, offer.offer_name as offer_name, offer.offer_description as description, offer.offer_subcategoryId,
            {CaseCatAgg}
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offer` offer 
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


def get_insert_tags_request(offers_tagged):

    bigquery_query = f"INSERT INTO {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.offer_tags (offer_id,tag) VALUES "
    query = ""
    for index, row in offers_tagged.iterrows():
        if isinstance(row["tag"], list):
            query += ",".join(
                [f"""("{row['offer_id']}","{tag}")""" for tag in row["tag"]]
            )
        else:
            query += f"""("{row['offer_id']}","{row["tag"]}")"""
        query += ","
    bigquery_query += query[:-1] + ";"
    return bigquery_query


def insert_to_table(offers_tagged):
    bigquery_client = BigQueryClient()
    if offers_tagged.shape[0] > OFFERS_TO_TAG_MAX_LENGTH:
        nb_df_sub_divisions = offers_tagged.shape[0] // OFFERS_TO_TAG_MAX_LENGTH
        for k in range(nb_df_sub_divisions):
            bigquery_query = get_insert_tags_request(
                offers_tagged[
                    k * OFFERS_TO_TAG_MAX_LENGTH : (k + 1) * OFFERS_TO_TAG_MAX_LENGTH
                ]
            )
            bigquery_client.query(bigquery_query)

        bigquery_query = get_insert_tags_request(
            offers_tagged[(nb_df_sub_divisions) * OFFERS_TO_TAG_MAX_LENGTH :]
        )
        bigquery_client.query(bigquery_query)
    else:
        bigquery_query = get_insert_tags_request(offers_tagged)
        bigquery_client.query(bigquery_query)
    return


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


def get_offers_to_tag():
    save_to_csv(pd.read_gbq(get_offers_to_tag_request()), FILENAME_INITIAL)
    return


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
    save_to_csv(
        extract_tags_offer_name(fetch_offers_to_tag()),
        FILENAME_OFFER_NAME
        # extract_tags_offer_name(load_from_csv(FILENAME_INITIAL)), FILENAME_OFFER_NAME
    )
    return


def merge_dataframes(df1, df2):
    return pd.concat([df1, df2], ignore_index=True)


def save_to_csv(dataframe, filename):
    dataframe.to_csv(f"gs://{filename}")
    return


def load_from_csv(filename):
    return pd.read_csv(f"gs://{filename}")


def update_table():
    # dfinit table with all offers to tag not present in offer_tags
    df_offers_to_tag = fetch_offers_to_tag()
    df_description_tags = load_from_csv(FILENAME_DESCRIPTION)
    df_offer_name_tags = load_from_csv(FILENAME_OFFER_NAME)

    # df12 merge of offer_name and description
    df_all_tags = merge_dataframes(df_description_tags, df_offer_name_tags)

    # df3 offer wO tags
    df_offers_wo_tags = df_offers_to_tag[
        ~df_offers_to_tag.offer_id.isin(df_all_tags.offer_id)
    ].assign(tag="none")

    # df_final , should be the same as dfinit but all the offers have a tag or 'none'
    df_offers_tagged = merge_dataframes(df_all_tags, df_offers_wo_tags)
    insert_to_table(df_offers_tagged)

    return
