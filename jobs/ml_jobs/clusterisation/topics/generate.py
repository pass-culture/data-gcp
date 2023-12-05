import typer
import pandas as pd

from bertopic import BERTopic
from bertopic.representation import (
    MaximalMarginalRelevance,
    PartOfSpeech,
)
from tqdm import tqdm
import numpy as np


from tools.utils import ENV_SHORT_NAME, GCP_PROJECT_ID
from tools.prompts import EXPECTED_RESULTS, SYSTEM_PROMPT
from tools.utils import call_retry


def load_df():
    return pd.read_gbq(
        f"""
        WITH offer_booking AS (
            SELECT
                item_id,
                sum(booking_cnt) as booking_cnt
            FROM `{GCP_PROJECT_ID}.analytics_{ENV_SHORT_NAME}.enriched_offer_data`
            GROUP BY 1
        )

        SELECT 
            ic.cluster as cluster_id,
            ic.cluster_name as semantic_category, 
            TO_BASE64(SHA1(ic.cluster_name)) as semantic_cluster_id,
            ic.x_cluster,
            ic.y_cluster,
            ic.item_id,
            
            ic.semantic_encoding,
            if(ei.offer_name = 'None', '', ei.offer_name) as offer_name,
            if(ei.offer_description = 'None', '', ei.offer_description) as offer_description,
            ob.booking_cnt
        FROM `{GCP_PROJECT_ID}.clean_{ENV_SHORT_NAME}.item_clusters` ic

        LEFT JOIN `{GCP_PROJECT_ID}.analytics_{ENV_SHORT_NAME}.enriched_item_metadata` ei on ei.item_id = ic.item_id
        LEFT JOIN offer_booking ob on ob.item_id = ic.item_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ic.item_id ORDER BY ic.cluster ASC) = 1
    """
    )


def generate_topics(topic_items_df, semantic_cluster_id):
    selected_topic = topic_items_df[
        topic_items_df.semantic_cluster_id == semantic_cluster_id
    ].reset_index(drop=True)
    selected_topic["raw_text"] = (
        selected_topic["offer_name"].astype(str)
        + " "
        + selected_topic["offer_description"].astype(str)
    )
    embeddings = np.hstack(selected_topic["semantic_encoding_emb"]).reshape(-1, 5)
    mmr = MaximalMarginalRelevance(diversity=0.3, top_n_words=20)
    pos = PartOfSpeech("fr_core_news_sm", top_n_words=20)
    representation_models = [pos, mmr]

    topic_model = BERTopic(
        language="multilingual",
        top_n_words=25,
        # min_topic_size=min_topic_size,
        nr_topics=10,
        representation_model=representation_models,
        # nr_topics="auto",
        calculate_probabilities=False,
    )
    topics, probs = topic_model.fit_transform(
        selected_topic["raw_text"].astype(str), embeddings=embeddings
    )
    results = topic_model.get_document_info(selected_topic["raw_text"]).reset_index(
        drop=True
    )
    results.columns = [x.lower() for x in results.columns]
    out = pd.concat([results, selected_topic], axis=1)
    return out


def preproc_topics(topic_model, semantic_cluster_id):
    topics_info = topic_model[
        ["name", "representation", "topic", "representative_docs", "category"]
    ].drop_duplicates(subset=["topic", "name"])
    topics = []
    for row in topics_info.itertuples():
        topics.append(
            {
                "category": row.category,
                "semantic_cluster_id": semantic_cluster_id,
                "topic_terms": ",".join(row.representation),
                "name": row.name,
                "topic": row.topic,
                "docs": "|".join([x[:200] for x in row.representative_docs]),
            }
        )
    return topics


def loop_gpt(topics):
    for terms in topics:
        topic_term = terms["topic_terms"]
        doc = terms["docs"]

        messages = SYSTEM_PROMPT + [
            {
                "role": "user",
                "content": f"""
            Trouvez un genre ou une catégorie culturelle qui est la plus commune dans les mots ci-dessous. 
            Les mots du début sont plus important que ceux de la fin.
             ``` {topic_term} ```
            Voici des documents représentatifs: 
            ``` {doc} ```  
            Donnez un terme pour chaque type.
            Return JSON {{"category" : <xxx>, "sub_category" : <xxx>, "medium" : <xxx>, "genre": <xxx>, "sub_genre": <xxx>}}
        """,
            },
        ]
        categories = call_retry(
            messages,
            test_fn=lambda x: all([x in EXPECTED_RESULTS for x in list(x.keys())])
            and len(x) == len(EXPECTED_RESULTS),
            ttl=60,
            temperature=0.8,
        )
        for k, v in categories.items():
            if v is None or v == "":
                v = "N/A"
            categories[k] = v
            terms[f"micro_{k}"] = v

        terms["micro_category_details"] = ",".join(categories.values())

    topics_df = pd.DataFrame(topics)

    topics_micro = " | ".join(topics_df["micro_category"].values)

    messages = SYSTEM_PROMPT + [
        {
            "role": "user",
            "content": f"""
            Trouvez les genre et les catégories culturelles qui sont les plus communes dans les mots ci-dessous. 
            ``` {topics_micro} ```
            Return JSON {{"category" : <xxx>, "sub_category" : <xxx>, "medium" : <xxx>, "genre": <xxx>, "sub_genre": <xxx>}}
        """,
        },
    ]
    categories = call_retry(
        messages,
        test_fn=lambda x: all([x in EXPECTED_RESULTS for x in list(x.keys())])
        and len(x) == len(EXPECTED_RESULTS),
        ttl=60,
        temperature=0.8,
    )
    categories = categories or {}
    for k, v in categories.items():
        if v is None or v == "":
            v = "N/A"
        categories[k] = v
        topics_df[f"macro_{k}"] = v

    topics_df["macro_category_details"] = ",".join(list(categories.values()))
    return topics_df


def main():
    topic_items_df = load_df()
    topic_items_df["category"] = (
        topic_items_df["semantic_category"].str.split("-", 1).str[0]
    )

    count_df = (
        topic_items_df.groupby(["semantic_cluster_id", "semantic_category"])
        .agg(
            {
                "x_cluster": "mean",
                "y_cluster": "mean",
                "booking_cnt": "sum",
                "item_id": "count",
            }
        )
        .reset_index()
    )
    count_df.columns = [
        "semantic_cluster_id",
        "semantic_category",
        "x_cluster_mean",
        "y_cluster_mean",
        "booking_cluster_cnt",
        "item_id_cluster_cnt",
    ]
    topics_raw_all_df = []
    topics_all_df = []

    for semantic_cluster_id in tqdm(list(count_df["semantic_cluster_id"].unique())):
        topic_raw_df = generate_topics(topic_items_df, semantic_cluster_id)
        topics = preproc_topics(topic_raw_df)
        topics_df = loop_gpt(topics)
        topics_raw_all_df.append(topic_raw_df)
        topics_all_df.append(topics_df)

    topics_all_df = pd.concat(topics_all_df).merge(count_df, on=["semantic_cluster_id"])
    topics_raw_all_df = pd.concat(topics_raw_all_df)
    topics_all_df.to_gbq(
        f"tmp_{ENV_SHORT_NAME}.item_topics_all_df", if_exists="replace"
    )
    topics_raw_all_df.to_gbq(
        f"tmp_{ENV_SHORT_NAME}.item_topics_raw_all_df", if_exists="replace"
    )


if __name__ == "__main__":
    typer.run(main)
