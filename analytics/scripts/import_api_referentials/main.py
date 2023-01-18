import pandas as pd
import pandas_gbq as gbq
import importlib
import argparse


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
        "pcapi.core.categories.subcategories"
    ).ALL_SUBCATEGORIES
    export_subcat = []
    for subcats in subcategories:
        export_subcat.append(subcats.__dict__)
    df = pd.DataFrame(export_subcat)
    df.rename(columns={"category": "category_id"})
    dtype_list = list(df.columns)
    for k, v in CATEGORIES_DTYPES.items():
        if k in dtype_list:
            df[k] = df[k].astype(v)
    df.to_gbq(
        f"""analytics_{env_short_name}.subcategories""",
        project_id=gcp_project_id,
        if_exists="replace",
    )


def get_types(gcp_project_id, env_short_name):
    show_types = importlib.import_module("pcapi.domain.show_types").show_types
    music_types = importlib.import_module("pcapi.domain.music_types").music_types

    types = {"show": show_types, "music": music_types}
    export_types = []
    for k, types_list in types.items():
        for _t in types_list:
            code = _t.code
            label = _t.label
            for _c in _t.children:
                export_types.append(
                    {
                        "domain": k,
                        "type": code,
                        "label": label,
                        "sub_type": _c.code,
                        "sub_label": _c.label,
                    }
                )
    df = pd.DataFrame(export_types)
    dtype_list = list(df.columns)
    for k, v in TYPES_DTYPES.items():
        if k in dtype_list:
            df[k] = df[k].astype(v)
    df.to_gbq(
        f"""analytics_{env_short_name}.offer_types""",
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
