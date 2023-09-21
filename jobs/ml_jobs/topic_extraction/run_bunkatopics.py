from bunkatopics import Bunka
from langchain.embeddings import HuggingFaceEmbeddings
import pandas as pd
import random

from tools.config import ENV_SHORT_NAME

semantic_content = ["offer_name", "offer_description"]

# Chose an embedding model
embedding_model = HuggingFaceEmbeddings(
    model_name="distiluse-base-multilingual-cased-v2"
)


def main(
    splitting: float = typer.Option(
        ..., help="Split proportion between fit and predict"
    ),
    input_table_name: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
    ),
    output_table_name: str = typer.Option(
        ...,
        help="Name of the cleaned dataframe",
    ),
) -> None:
    # Load product description, example with description from the Arts category
    clusters = [150]
    # clusters = [30, 150]
    categories = ["ARTS", "MUSIQUE"]
    # categories = ["ARTS", "CINEMA", "LIVRES", "MANGA", "MUSIQUE", "SORTIES"]

    # f"{RESULTS_PATH}/topics/topic_clusters/dfclusters_desc_{category}_{n_cluster}.csv",
    final = []
    # for category in tqdm(categories):
    category = "ALL"
    for n_cluster in clusters:
        print("RESULTS_PATH: ", RESULTS_PATH)

        data = pd.read_gbq(f"SELECT * FROM `tmp_{ENV_SHORT_NAME}.{input_table_name}`")
        docs_all_cluster = []
        topics_all_cluster = []
        for cluster_id in data["cluster"].unique().tolist():
            data_cluster = get_clean_cluster_data(data, cluster_id)
            full_docs = list(data_cluster["semantic_content"])
            bunka = Bunka(model_hf=embedding_model, language="fr_core_news_sm")
            # print("len(full_docs): ",len(full_docs))
            random.shuffle(full_docs)
            row_count_splitting = int((len(full_docs) + 1) * splitting)
            fitted_doc = full_docs[:row_count_splitting]
            # predicted_doc=full_docs[splitting:]
            if len(fitted_doc) < 10:
                print("Continue..")
                continue
            print(f"Fitting cluster:{cluster_id}...")
            bunka.fit(fitted_doc)
            # bunka.fit(docs=full_docs, ids=ids)
            df_topics = bunka.get_topics(n_clusters=2)
            # print("len(df_topics): ",len(df_topics))
            # The main output are the list of terms that describe the topics

            # df_topics.to_csv(RESULTS_PATH + f"/topics/topic_representation/topic_by_precalculated_clusters/topics_{category}_clusters_{cluster_id}.csv")
            # Load docs and their indexed topics
            topic_terms = " | ".join(df_topics["name"].tolist())
            docs = bunka.docs
            df_docs, df_topics_tmp = get_cluster_topics_and_docs(
                cluster_id, data_cluster, topic_terms, docs
            )

            item_with_topic = pd.merge(
                df_docs, data_cluster, on=["semantic_content"], how="inner"
            )
            docs_all_cluster.append(item_with_topic)
            topics_all_cluster.append(df_topics_tmp)
            already_df = [
                df_topics,
                df_docs,
                data_cluster,
                item_with_topic,
                df_topics_tmp,
            ]
            del already_df
        df_docs_all_cluster = pd.concat(docs_all_cluster, ignore_index=True)
        df_topics_all_cluster = pd.concat(topics_all_cluster, ignore_index=True)

        # df_docs_all_cluster.to_csv(RESULTS_PATH + f"/topics/topic_representation/topic_by_precalculated_clusters/topics_{category}_{n_cluster}_clusters_DOCS.csv")
        df_topics_all_cluster.to_gbq(f"clean_{ENV_SHORT_NAME}.{output_table_name}")
        df_docs_all_cluster.to_gbq(f"clean_{ENV_SHORT_NAME}.{output_table_name}_docs")


def get_cluster_topics_and_docs(cluster_id, data_cluster, topic_terms, docs):
    doc_id = [doc.doc_id for doc in docs]
    content = [doc.content for doc in docs]
    emb = [doc.embedding for doc in docs]
    topic_id = [cluster_id for doc in docs]
    term_id = [doc.term_id for doc in docs]
    df_docs = pd.DataFrame(
        {
            "topic_id": topic_id,
            "topic_terms": topic_terms,
            "doc_id": doc_id,
            "term_id": term_id,
            "semantic_content": content,
            "doc_embedding": emb,
        }
    )
    df_topics_tmp = pd.DataFrame(
        {
            "topic_id": [cluster_id],
            "topic_terms": [topic_terms],
            "category": [" | ".join(data_cluster.category.tolist())],
        }
    )

    return df_docs, df_topics_tmp


def extract_topics_metadata(cluster_id, docs):
    doc_id = [doc.doc_id for doc in docs]
    content = [doc.content for doc in docs]
    emb = [doc.embedding for doc in docs]
    topic_id = [cluster_id for doc in docs]
    term_id = [doc.term_id for doc in docs]
    return doc_id, content, emb, topic_id, term_id


def get_clean_cluster_data(data, cluster_id):
    data_cluster = data[data["cluster"] == cluster_id]
    data_cluster = data_cluster.fillna("")
    data_cluster["semantic_content"] = data_cluster[semantic_content].agg(
        "|".join, axis=1
    )
    data_cluster["semantic_content"] = data_cluster[
        "semantic_content"
    ].drop_duplicates()
    return data_cluster


if __name__ == "__main__":
    typer.run(main)
