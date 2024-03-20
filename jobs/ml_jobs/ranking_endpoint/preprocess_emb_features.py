import json
import pandas as pd
from datetime import datetime
import numpy as np
import typer
from utils import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
)

from config.preprocess_config import params


def load_data(dataset_name, table_name, column_name):
    sql = f"""WITH item_ids as(
    SELECT
        distinct offer_item_id as item_id
        FROM `{GCP_PROJECT_ID}.{dataset_name}.past_offer_context` poc 
        WHERE event_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
        AND poc.user_id != "-1" 
        AND poc.item_rank <= 30
        group by 1
    )
    SELECT ii.item_id,{column_name} as emb_feature
    from item_ids ii
    JOIN `{GCP_PROJECT_ID}.{dataset_name}.{table_name}` using(item_id)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY extraction_date DESC) = 1
    """
    print(sql)
    return pd.read_gbq(sql).sample(frac=1)


def convert_str_emb_to_float(emb_list, emb_size=124):
    float_emb = []
    for str_emb in emb_list:
        try:
            emb = json.loads(str_emb)
        except:
            emb = [0] * emb_size
        float_emb.append(np.array(emb))
    return float_emb


def main(
    feature_name: str = typer.Option(
        None,
        help="Name of the features to be preprocess",
    ),
) -> None:

    table_name = params[feature_name]["table_name"]
    dataset_name = params[feature_name]["dataset_name"]
    column_name = params[feature_name]["column_name"]
    df_emb_feature = load_data(
        dataset_name=dataset_name, table_name=table_name, column_name=column_name
    )
    item_sem_emb_list_float = convert_str_emb_to_float(
        df_emb_feature["emb_feature"].tolist()
    )
    item_emb_mean = pd.DataFrame(
        {
            "item_id": df_emb_feature.item_id.tolist(),
            f"{feature_name}_emb_mean": [emb.mean() for emb in item_sem_emb_list_float],
        }
    )
    item_emb_mean.to_gbq(
        f"""clean_{ENV_SHORT_NAME}.{feature_name}_emb_mean""",
        project_id=GCP_PROJECT_ID,
        if_exists="replace",
    )


if __name__ == "__main__":
    typer.run(main)
