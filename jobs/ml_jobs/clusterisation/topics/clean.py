import pandas as pd
import typer

from polyfuzz.models import Embeddings
from polyfuzz import PolyFuzz

from flair.embeddings import TransformerWordEmbeddings
from configs.labels import CAT, GENRE, MEDIUM
from tools.utils import CLEAN_DATASET, TMP_DATASET, load_config_file

from tqdm import tqdm

LIST_TO_MAP = {
    "category_lvl1": {"in_column": "sub_category_details", "target": CAT},
    "category_medium_lvl1": {"in_column": "medium_details", "target": MEDIUM},
    "category_genre_lvl1": {"in_column": "genre_details", "target": GENRE},
    "category_lvl2": {
        "in_column": "micro_sub_category",
    },
    "category_medium_lvl2": {
        "in_column": "micro_medium",
    },
    "category_genre_lvl2": {
        "in_column": "micro_genre",
    },
}


def clean_row(row):
    if len(row) < 3:
        return "N/A"
    return row


def match_with_target(df, input_column, target_list, target_column):
    list_to_match = list(df[input_column].unique())

    embeddings = TransformerWordEmbeddings("bert-base-multilingual-cased")
    bert = Embeddings(embeddings, min_similarity=0.4, model_id="BERT")
    model = PolyFuzz(bert)

    model.match(list_to_match, to_list=target_list)
    model.group(link_min_similarity=0.7, group_all_strings=True)
    result_df = model.get_matches()
    result_df.columns = [input_column, "to", "similarity", target_column]

    df = df.merge(
        result_df[[input_column, target_column]].drop_duplicates(),
        on=input_column,
        how="left",
    )
    df[target_column] = df[target_column].combine_first(df[input_column])

    return df


def clean_words(df, input_column, target_column):
    list_to_match = list(df[input_column].unique())
    embeddings = TransformerWordEmbeddings("bert-base-multilingual-cased")
    bert = Embeddings(embeddings, min_similarity=0.4, model_id="BERT")

    model = PolyFuzz(bert)
    model.match(list_to_match)
    model.group(link_min_similarity=0.70, group_all_strings=True)
    result_df = model.get_matches()
    result_df.columns = ["input", input_column, "similarity", target_column]
    df = df.merge(
        result_df[[input_column, target_column]].drop_duplicates(),
        on=input_column,
        how="left",
    )
    df[target_column] = df[target_column].combine_first(df[input_column])
    return df


def load_df(input_table):
    return pd.read_gbq(f"""SELECT * FROM `{TMP_DATASET}.{input_table}`""")


def main(
    item_topics_labels_input_table: str = typer.Option(..., help="Path to data"),
    item_topics_input_table: str = typer.Option(..., help="Path to data"),
    item_topics_labels_output_table: str = typer.Option(..., help="Path to data"),
    item_topics_output_table: str = typer.Option(..., help="Path to data"),
    config_file_name: str = typer.Option(
        "default-config",
        help="Config file name",
    ),
):
    df = load_df(item_topics_labels_input_table)
    params = load_config_file(config_file_name)

    df["category_details"] = df["macro_category"] + " " + df["micro_category"]
    df["sub_category_details"] = (
        df["macro_sub_category"] + " " + df["micro_sub_category"]
    )
    df["genre_details"] = df["macro_genre"] + " " + df["micro_genre"]
    df["medium_details"] = df["macro_medium"] + " " + df["micro_medium"]

    df_raw = load_df(item_topics_input_table)

    df = match_with_target(
        df,
        input_column="category_details",
        target_list=list(CAT.keys()),
        target_column="category_lvl0",
    )

    post_proc = []

    for lvl_0 in list(CAT.keys()):
        print(f"Categorize {lvl_0}")
        _df = df[df["category_lvl0"] == lvl_0].reset_index(drop=True)
        if not _df.empty:
            for output_col, params_details in tqdm(LIST_TO_MAP.items()):
                target_cat = params_details.get("target", None)
                in_column = params_details.get("in_column", None)
                _df[in_column] = _df[in_column].apply(clean_row)
                if target_cat is not None:
                    _df = match_with_target(
                        _df,
                        input_column=in_column,
                        target_list=target_cat[lvl_0],
                        target_column=output_col,
                    )
                else:
                    _df = clean_words(
                        _df, input_column=in_column, target_column=output_col
                    )
            post_proc.append(_df)

    post_proc_df = pd.concat(post_proc)[
        [
            "semantic_cluster_id",
            "docs",
            "topic_terms",
            "semantic_category",
            "topic_id",
            "category_lvl0",
            "category_lvl1",
            "category_medium_lvl1",
            "category_genre_lvl1",
            "category_lvl2",
            "category_medium_lvl2",
            "category_genre_lvl2",
            "micro_category_details",
            "macro_category_details",
            "x_cluster_mean",
            "y_cluster_mean",
            "booking_cluster_cnt",
            "item_id_cluster_cnt",
        ]
    ]
    post_proc_df.to_gbq(
        f"{CLEAN_DATASET}.{item_topics_labels_output_table}", if_exists="replace"
    )

    df_raw.merge(
        post_proc_df[
            [
                "semantic_cluster_id",
                "topic_id",
                "category_lvl0",
                "category_lvl1",
                "category_medium_lvl1",
                "category_genre_lvl1",
                "category_lvl2",
                "category_medium_lvl2",
                "category_genre_lvl2",
            ]
        ],
        on=["semantic_cluster_id", "topic_id"],
    ).to_gbq(f"{CLEAN_DATASET}.{item_topics_output_table}", if_exists="replace")


if __name__ == "__main__":
    typer.run(main)
