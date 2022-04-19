import pandas as pd
import pandas_gbq as gbq
from utils import STORAGE_PATH, MODEL_NAME, GCP_PROJECT_ID, ENV_SHORT_NAME


def split_data(storage_path: str, model_name: str):
    if model_name == "v1" or model_name == "v2_mf_reco":
        clean_data = pd.read_csv(f"{storage_path}/clean_data.csv")

        df = clean_data.sample(frac=1).reset_index(drop=True)
        lim_train = df.shape[0]
        lim_eval = df.shape[0] * 90 / 100
        positive_data_train = df.loc[df.index < lim_train]
        positive_data_eval = df.loc[df.index < lim_eval]
        positive_data_eval = positive_data_eval.loc[
            positive_data_eval.index >= lim_train
        ]
        positive_data_test = df[df.index >= lim_eval]
        positive_data_train.to_csv(
            f"{storage_path}/positive_data_train.csv", index=False
        )
        positive_data_test.to_csv(f"{storage_path}/positive_data_test.csv", index=False)
        positive_data_eval.to_csv(f"{storage_path}/positive_data_eval.csv", index=False)

    if model_name == "v2_deep_reco":
        clicks_light = pd.read_csv(f"{storage_path}/clean_data.csv")
        clicks_train_light = clicks_light[clicks_light.train_set == True]
        clicks_test_light = clicks_light[clicks_light.train_set == False]
        clicks_train_light.to_csv(
            f"{storage_path}/positive_data_train.csv", index=False
        )

        clicks_test_light.to_csv(f"{storage_path}/positive_data_test.csv", index=False)

        clicks_train_light[
            [
                "user_id",
                "offer_id",
                "offer_subcategoryid",
                "item_id",
                "click_count",
                "train_set",
            ]
        ].to_gbq(
            f"""clean_{ENV_SHORT_NAME}.temp_positive_data_train""",
            project_id=f"{GCP_PROJECT_ID}",
            if_exists="replace",
        )


if __name__ == "__main__":
    split_data(STORAGE_PATH, MODEL_NAME)
