import pandas as pd


def lighten_matrice(storage_path: str):
    clicks_df = pd.read_csv(f"{storage_path}/raw_data.csv")
    top_offers = clicks_df.item_id.value_counts()
    top_offers = top_offers[top_offers > 40].index.tolist()
    clicks_df_light = clicks_df[clicks_df.item_id.isin(top_offers)]

    top_users = clicks_df.user_id.value_counts()
    top_users = top_users[top_users > 10].index.tolist()
    clicks_df_light = clicks_df_light[clicks_df_light.user_id.isin(top_users)]
    clicks_df_light.to_csv(f"{storage_path}/clean_data.csv")
    return
