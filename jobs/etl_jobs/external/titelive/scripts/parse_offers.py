# %%

import json

import pandas as pd

df = pd.read_parquet("data/yesterday_books.parquet")


def post_process(df: pd.DataFrame):
    return df.replace({"idserie": {"0": None}}).astype({"taux_tva": float})


# %%

# df["data"].apply(json.loads)
# a = pd.DataFrame(df["data"].apply(json.loads).to_list())
# b = pd.json_normalize(df["data"].apply(json.loads).to_list())
# c = df["data"].apply(json.loads).to_list()


exploded_df = (
    pd.DataFrame(df["data"].apply(json.loads).to_list())
    .assign(
        article_length=lambda _df: _df.article.map(len),
        first_article=lambda _df: _df.article.map(
            lambda o: min([key for key in o.keys()])
        ),
        article_list=lambda _df: _df.article.map(lambda o: list(o.values())),
    )
    .explode("article_list")
)
exploded_df


# %%
def post_process_before_saving(df: pd.DataFrame):
    # Convert dict like columns to JSON
    for col in df.select_dtypes(include=[object]).columns:
        if df[col].dropna().apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].map(json.dumps).replace(["None", "nan", "NaN"], None)

        if col == "article_taux_tva":
            df[col] = df[col].astype(float)

        if col in ["article_image", "article_iad"]:
            df[col] = df[col].astype(int)

    return df


merged_df = (
    pd.concat(
        [
            exploded_df.drop(columns=["article_list"]),
            exploded_df["article_list"].apply(pd.Series).add_prefix("article_"),
        ],
        axis=1,
    )
    .assign(
        auteurs_multi=lambda df: df.auteurs_multi.map(json.dumps),
    )
    .drop(columns=["article"])
)
merged_df
pp_df = merged_df.pipe(post_process_before_saving)


pp_df.to_parquet("data/yesterday_books_parsed.parquet")

# %%
