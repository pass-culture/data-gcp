import argparse
import importlib
import unicodedata

import numpy as np
import pandas as pd

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


def get_subcategories(gcp_project_id, env_short_name):
    subcategories = importlib.import_module(
        "pcapi.core.categories.subcategories_v2"
    ).ALL_SUBCATEGORIES
    export_subcat = []
    for subcats in subcategories:
        _exports = {
            "id": subcats.id,
            "category_id": subcats.category.id,
            "pro_label": subcats.pro_label,
            "app_label": subcats.app_label,
            "search_group_name": subcats.search_group_name,
            "homepage_label_name": subcats.homepage_label_name,
            "is_event": subcats.is_event,
            "conditional_fields": [k for k, v in subcats.conditional_fields.items()],
            "can_expire": subcats.can_expire,
            "is_physical_deposit": subcats.is_physical_deposit,
            "is_digital_deposit": subcats.is_digital_deposit,
            "online_offline_platform": subcats.online_offline_platform,
            "reimbursement_rule": subcats.reimbursement_rule,
            "can_be_duo": subcats.can_be_duo,
            "can_be_educational": subcats.can_be_educational,
            "is_selectable": subcats.is_selectable,
            "is_bookable_by_underage_when_free": subcats.is_bookable_by_underage_when_free,
            "is_bookable_by_underage_when_not_free": subcats.is_bookable_by_underage_when_not_free,
            "can_be_withdrawable": subcats.can_be_withdrawable,
        }
        export_subcat.append(_exports)
    df = pd.DataFrame(export_subcat)
    dtype_list = list(df.columns)
    for k, v in CATEGORIES_DTYPES.items():
        if k in dtype_list:
            df[k] = df[k].astype(v)

    df.to_gbq(
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
