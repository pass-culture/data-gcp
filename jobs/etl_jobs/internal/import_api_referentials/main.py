import argparse
import importlib
import unicodedata

import numpy as np
import pandas as pd
import requests

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


def get_subcategories(gcp_project_id: str, env_short_name: str) -> None:
    # Read deprecated data from CSV (kept for reference)
    deprecated_subactegories = pd.read_csv("data/subcategories_v2_20250327.csv")

    # Fetch current subcategories from API
    response = requests.get(
        "https://backend.passculture.app/native/v1/subcategories/v2"
    )
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


def get_types(gcp_project_id, env_short_name):
    show_types = importlib.import_module("pcapi.domain.show_types").SHOW_TYPES
    music_types = importlib.import_module("pcapi.domain.music_types").OLD_MUSIC_TYPES
    book_types = importlib.import_module("pcapi.domain.book_types").BOOK_MACRO_SECTIONS
    movie_types = importlib.import_module("pcapi.domain.movie_types").MOVIE_TYPES

    types = {
        "show": show_types,
        "music": music_types,
        "book": book_types,
        "movie": movie_types,
    }
    export_types = []
    for domain, types_list in types.items():
        if domain in ["show", "music"]:
            for _t in types_list:
                code = _t.code
                label = _t.label
                for _c in _t.children:
                    export_types.append(
                        {
                            "domain": domain,
                            "type": code,
                            "label": label,
                            "sub_type": _c.code,
                            "sub_label": _c.label,
                        }
                    )
        elif domain == "book":
            for type_label in types_list:
                type_id = "".join(
                    letter for letter in type_label.lower() if letter.isalnum()
                )
                export_types.append(
                    {
                        "domain": domain,
                        "type": str(
                            unicodedata.normalize("NFD", type_id)
                            .encode("ascii", "ignore")
                            .decode("utf-8")
                        ),
                        "label": type_label,
                        "sub_type": np.nan,
                        "sub_label": np.nan,
                    }
                )
        elif domain == "movie":
            for _t in types_list:
                type_id = _t.name
                type_label = _t.label
                export_types.append(
                    {
                        "domain": domain,
                        "type": type_id,
                        "label": type_label,
                        "sub_type": np.nan,
                        "sub_label": np.nan,
                    }
                )

    df = pd.DataFrame(export_types)
    dtype_list = list(df.columns)
    for k, v in TYPES_DTYPES.items():
        if k in dtype_list:
            df[k] = df[k].astype(v)
    df.to_gbq(
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
    if job_type == "subcategories":
        get_subcategories(gcp_project_id, env_short_name)
    elif job_type == "types":
        get_types(gcp_project_id, env_short_name)
    else:
        raise Exception(
            f"Job type not found. Got {job_type}, expecting subcategories|types."
        )
    print(f"Export for {job_type} done.")
