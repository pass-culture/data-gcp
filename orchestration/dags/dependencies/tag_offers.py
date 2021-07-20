import pandas as pd
import pandas_gbq as gbq
from dependencies.bigquery_client import BigQueryClient
from dependencies.config import GCP_PROJECT, BIGQUERY_CLEAN_DATASET

CaseCatAgg = """CASE
                when offer.offer_type ='ThingType.AUDIOVISUEL' then 'Audiovisuel'
                when offer.offer_type = 'ThingType.OEUVRE_ART' then 'Autre'
                when offer.offer_type ='EventType.JEUX' then 'Autre'
                when offer.offer_type ='EventType.CONFERENCE_DEBAT_DEDICACE' then 'Autre'
                when offer.offer_type ='ThingType.CINEMA_CARD' then 'Cinéma'
                when offer.offer_type ='EventType.CINEMA' then 'Cinéma'
                when offer.offer_type = 'ThingType.CINEMA_ABO' then 'Cinéma'
                when offer.offer_type = 'ThingType.INSTRUMENT' then 'Instrument'
                when offer.offer_type = 'ThingType.JEUX_VIDEO' then 'Jeux vidéo'
                when offer.offer_type = 'ThingType.JEUX_VIDEO_ABO' then 'Jeux vidéo'
                when offer.offer_type ='ThingType.LIVRE_AUDIO' then 'Livre'
                when offer.offer_type ='ThingType.LIVRE_EDITION' then 'Livre'
                when offer.offer_type ='EventType.MUSEES_PATRIMOINE' then 'Musée-patrimoine'
                when offer.offer_type ='ThingType.MUSEES_PATRIMOINE_ABO' then 'Musée-patrimoine'
                when offer.offer_type ='ThingType.MUSIQUE' then 'Musique'
                when offer.offer_type ='ThingType.MUSIQUE_ABO' then 'Musique'
                when offer.offer_type ='EventType.MUSIQUE' then 'Musique'
                when offer.offer_type ='ThingType.PRATIQUE_ARTISTIQUE_ABO' then 'Pratique-artistique'
                when offer.offer_type ='EventType.PRATIQUE_ARTISTIQUE' then 'Pratique-artistique'
                when offer.offer_type ='ThingType.PRESSE_ABO' then 'Presse'
                when offer.offer_type ='EventType.SPECTACLE_VIVANT' then 'Spectacle-vivant'
                when offer.offer_type ='ThingType.SPECTACLE_VIVANT_ABO' then 'Spectacle-vivant'
                else 'Autre'
            END as categorie_principale """

TagDict = {
    "Instrument": [
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
    "Musée-patrimoine": [
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
    "Spectacle-vivant": [
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
    "Musique": [
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
    "Pratique-artistique": [
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


def get_offers_to_tag_request(category):
    return f"""WITH extra_data_description AS (
            SELECT offer_extra_data.offer_id as offer_ID, offer.offer_description as description, offer.offer_type,
            {CaseCatAgg}
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.offer_extracted_data` offer_extra_data
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offer` offer ON offer_extra_data.offer_id = offer.offer_id
            )
            SELECT offer_ID, description FROM extra_data_description
            WHERE categorie_principale = '{category}'
            AND   description <> 'none'
            AND   description <> ""
            """


def get_update_tags_request(offers_tagged):

    bigquery_query = ""
    for index, row in offers_tagged.iterrows():

        query = "".join(
            f"""UPDATE `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.offer_extracted_data` SET offer_tags={row['tag']} WHERE offer_id="{row['offer_id']}"; """
        )
        bigquery_query += query

    return bigquery_query


def update_table(offers_tagged):
    bigquery_client = BigQueryClient()
    bigquery_query = get_update_tags_request(offers_tagged)
    bigquery_client.query(bigquery_query)


def TagDescriptions(offers_to_tag, TopicList):  # DataframeList[0]
    offer_tagged = []
    for index, row in offers_to_tag.iterrows():
        offer_id = row["offer_ID"]
        description = row["description"]
        descrip_dict = {"offer_id": offer_id}
        description_topic = []
        for word in TopicList:
            if word in description.lower():
                description_topic.append(word)
        if len(description_topic) > 0:
            descrip_dict["tag"] = description_topic
        else:
            descrip_dict["tag"] = [""]
        offer_tagged.append(descrip_dict)
    return pd.DataFrame(offer_tagged)


def tag_offers():
    offers_tagged = []
    for category in TAG_OFFERS_CATEGORIES:
        offers_to_tag = pd.read_gbq(get_offers_to_tag_request(category))
        offers_tagged.append(TagDescriptions(offers_to_tag, TagDict[f"{category}"]))
    offers_tagged_df = pd.concat(offers_tagged)
    offers_tagged_df.reset_index(drop=True, inplace=True)
    return offers_tagged_df


def extract_tags():
    update_table(tag_offers())
