import typer
import pandas as pd

from bertopic import BERTopic
from bertopic.representation import (
    MaximalMarginalRelevance,
    PartOfSpeech,
)
from tqdm import tqdm
import numpy as np


from tools.utils import ENV_SHORT_NAME, TMP_DATASET, CLEAN_DATASET, load_config_file
from tools.utils import call_retry, sha1_to_base64
from configs.prompts import (
    get_macro_topics_messages,
    get_micro_topics_messages,
    EXPECTED_RESULTS,
)


def load_df(input_table):
    return pd.read_gbq(f"""SELECT * FROM `{TMP_DATASET}.{input_table}`""")


def decode(x, emb_size):
    values = list(x.values())
    if len(values) == emb_size:
        return np.array(values)
    else:
        return None


def generate_topics(topic_items_df, semantic_cluster_id):
    selected_topic = topic_items_df[
        topic_items_df.semantic_cluster_id == semantic_cluster_id
    ].reset_index(drop=True)
    selected_topic["raw_text"] = (
        selected_topic["offer_name"].astype(str)
        + " "
        + selected_topic["offer_description"].astype(str)
    )
    embeddings = np.hstack(selected_topic["semantic_encoding"]).reshape(-1, 5)
    mmr = MaximalMarginalRelevance(diversity=0.5, top_n_words=20)
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
        messages = get_micro_topics_messages(topic_term, doc)
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

    messages = get_macro_topics_messages(topics_micro)
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


def main(
    input_table: str = typer.Option(..., help="Path to data"),
    item_topics_labels_output_table: str = typer.Option(..., help="Path to data"),
    item_topics_output_table: str = typer.Option(..., help="Path to data"),
    config_file_name: str = typer.Option(
        "default-config-offer",
        help="Config file name",
    ),
):
    topic_items_df = load_df(input_table)
    params = load_config_file(config_file_name)
    emb_size = params["pretrained_embedding_size"]

    topic_items_df["semantic_encoding"] = topic_items_df["semantic_encoding"].apply(
        lambda x: decode(x, emb_size)
    )
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
        try:
            topic_raw_df = generate_topics(topic_items_df, semantic_cluster_id)
            topics = preproc_topics(topic_raw_df, semantic_cluster_id)
            topics_df = loop_gpt(topics)
            topics_raw_all_df.append(topic_raw_df)
            topics_all_df.append(topics_df)
        except (ValueError, TypeError) as e:
            print(f"Error for {semantic_cluster_id}")
            print(e)

    topics_all_df = pd.concat(topics_all_df).merge(count_df, on=["semantic_cluster_id"])
    topics_raw_all_df = pd.concat(topics_raw_all_df)

    topics_all_df["topic_id"] = (
        topics_all_df["topic"].astype(str) + " " + topics_all_df["semantic_cluster_id"]
    )
    topics_all_df["topic_id"] = topics_all_df["topic_id"].apply(sha1_to_base64)

    topics_raw_all_df["topic_id"] = (
        topics_raw_all_df["topic"].astype(str)
        + " "
        + topics_raw_all_df["semantic_cluster_id"]
    )

    topics_raw_all_df["topic_id"] = topics_raw_all_df["topic_id"].apply(sha1_to_base64)
    topics_all_df.to_gbq(
        f"{TMP_DATASET}.{item_topics_labels_output_table}", if_exists="replace"
    )
    topics_raw_all_df[
        ["item_id", "topic_id", "semantic_cluster_id", "booking_cnt"]
    ].to_gbq(f"{TMP_DATASET}.{item_topics_output_table}", if_exists="replace")


if __name__ == "__main__":
    typer.run(main)
