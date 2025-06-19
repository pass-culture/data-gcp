import argparse
import unicodedata

import numpy as np
import pandas as pd
import requests

BACKEND_API_URL = {
    "dev": "https://backend.staging.passculture.team/native/v1/subcategories/v2",
    "stg": "https://backend.staging.passculture.team/native/v1/subcategories/v2",
    "prod": "https://backend.passculture.app/native/v1/subcategories/v2",
}

CATEGORIES_DTYPES = {
    "id": str,
    "category_id": str,
    "pro_label": str,
    "app_label": str,
    "search_group_name": str,
    "homepage_label_name": str,
    "is_event": bool,
    "conditional_fields": str,
    "can_expire": bool,
    "is_physical_deposit": bool,
    "is_digital_deposit": bool,
    "online_offline_platform": str,
    "reimbursement_rule": str,
    "can_be_duo": bool,
    "can_be_educational": bool,
    "is_selectable": bool,
    "is_bookable_by_underage_when_free": bool,
    "is_bookable_by_underage_when_not_free": bool,
    "can_be_withdrawable": bool,
}


TYPES_DTYPES = {
    "domain": str,
    "type": str,
    "label": str,
    "sub_type": str,
    "sub_label": str,
}


def convert_df_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    def _convert_camel_case_to_snake_case(camel_case: str) -> str:
        return "".join(
            ["_" + c.lower() if c.isupper() else c for c in camel_case]
        ).lstrip("_")

    return df.rename(
        columns={c: _convert_camel_case_to_snake_case(c) for c in df.columns}
    )


def clean_string(s):
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )
    s = s.replace("&", "")
    s = s.replace(",", "")
    s = s.replace("-", "")
    s = s.replace("/", "")
    s = s.replace(" ", "")
    return s


def get_subcategories(gcp_project_id: str, env_short_name: str, url: str) -> None:
    # Read deprecated data from CSV (kept for reference)
    deprecated_subactegories = pd.read_csv("data/subcategories_v2_20250327.csv")

    # Fetch current subcategories from API
    response = requests.get(url)
    response.raise_for_status()
    subcategories_data = response.json()
    new_subcategories_df = pd.DataFrame(subcategories_data["subcategories"]).pipe(
        convert_df_columns_to_snake_case
    )

    # Merge Both Dataframes
    COLUMNS_TO_KEEP = ["id"] + list(
        set(deprecated_subactegories.columns).difference(
            set(new_subcategories_df.columns)
        )
    )
    merged_df = deprecated_subactegories.loc[:, COLUMNS_TO_KEEP].merge(
        new_subcategories_df, left_on="id", right_on="id", how="inner"
    )

    if len(merged_df) != len(deprecated_subactegories):
        raise Exception(
            "Merging subcategories failed. Length of merged dataframe is not equal to the length of the deprecated subcategories dataframe."
        )

    if set(deprecated_subactegories.columns).difference(set(merged_df.columns)):
        raise Exception(
            "Merging subcategories failed. Columns are missing in the merged dataframe"
        )

    dtype_list = list(merged_df.columns)
    for k, v in CATEGORIES_DTYPES.items():
        if k in dtype_list:
            merged_df[k] = merged_df[k].astype(v)

    merged_df.to_gbq(
        f"""raw_{env_short_name}.subcategories""",
        project_id=gcp_project_id,
        if_exists="replace",
    )


def get_types(gcp_project_id: str, env_short_name: str, url: str) -> None:
    music_types = pd.read_csv("data/music_types_deprecated_20250401.csv")
    response = requests.get(url)
    response.raise_for_status()
    offer_types_data = response.json()

    for domain in offer_types_data["genreTypes"]:
        if domain["name"] == "BOOK":
            book_types = pd.DataFrame(domain["values"])

            book_types["domain"] = "book"
            book_types["type"] = book_types["name"].str.lower().apply(clean_string)
            book_types["label"] = book_types["name"]
            book_types["sub_type"] = np.nan
            book_types["sub_label"] = np.nan

            book_types = book_types[
                ["domain", "type", "label", "sub_type", "sub_label"]
            ]
        if domain["name"] == "SHOW":
            show_types = pd.DataFrame(domain["trees"])
            show_types_exploded = (
                show_types.explode("children")
                .reset_index(drop=True)
                .rename(columns={"code": "type"})
            )
            show_types_nested = pd.json_normalize(
                show_types_exploded["children"]
            ).rename(columns={"code": "sub_type", "label": "sub_label"})
            show_types = pd.concat(
                [show_types_exploded.drop(columns=["children"]), show_types_nested],
                axis=1,
            )
            show_types.drop(columns=["slug"], inplace=True)
            show_types["domain"] = "show"
            show_types = show_types[
                ["domain", "type", "label", "sub_type", "sub_label"]
            ]

        if domain["name"] == "MOVIE":
            movie_types = pd.DataFrame(domain["values"])
            movie_types.rename(columns={"name": "type", "value": "label"}, inplace=True)
            movie_types["sub_type"] = np.nan
            movie_types["sub_label"] = np.nan
            movie_types["domain"] = "movie"
            movie_types = movie_types[
                ["domain", "type", "label", "sub_type", "sub_label"]
            ]

    offer_types = pd.concat([music_types, book_types, show_types, movie_types], axis=0)
    dtype_list = list(offer_types.columns)
    for k, v in TYPES_DTYPES.items():
        if k in dtype_list:
            offer_types[k] = offer_types[k].astype(v)
    offer_types.to_gbq(
        f"""raw_{env_short_name}.offer_types""",
        project_id=gcp_project_id,
        if_exists="replace",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser("import_pcapi_model")
    parser.add_argument("--job_type", help="subcategories|types", type=str)
    parser.add_argument("--gcp_project_id", help="gcp_project_id", type=str)
    parser.add_argument("--env_short_name", help="env_short_name", type=str)
    args = parser.parse_args()
    job_type = args.job_type
    gcp_project_id = args.gcp_project_id
    env_short_name = args.env_short_name
    url = BACKEND_API_URL[env_short_name]
    if job_type == "subcategories":
        get_subcategories(gcp_project_id, env_short_name, url)
    elif job_type == "types":
        get_types(gcp_project_id, env_short_name, url)
    else:
        raise Exception(
            f"Job type not found. Got {job_type}, expecting subcategories|types."
        )
    print(f"Export for {job_type} done.")
