# %%

# # %% Debug

# import os
# import mlflow


# os.environ["MLFLOW_TRACKING_TOKEN"] = (
# )

# name = "compliance"
# model_type = "default"
# env = "dev"
# model_stage = "Production"
# MLFLOW_EHP_URI = "https://mlflow.staging.passculture.team/"
# model_name = f"{name}_{model_type}_{env}"
# mlflow.set_tracking_uri(MLFLOW_EHP_URI)
# catboost = mlflow.catboost.load_model(model_uri=f"models:/{model_name}/{model_stage}")
# catboost.save_model("catboost_model.cbm")
# %%
import json

from catboost import CatBoostClassifier

from fraud.offer_compliance_model.api_model import ApiModel

classification_model = CatBoostClassifier(one_hot_max_size=65).load_model(
    "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/algo_training/fraud/offer_compliance_model/compliance_model.cb"
)


with open(
    "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/algo_training/fraud/offer_compliance_model/configs/default.json",
    mode="r",
    encoding="utf-8",
) as config_file:
    features = json.load(config_file)

api_model = ApiModel(classification_model=classification_model, features=features)


# %%
model_input = {
    "offer_name": "test",
    "offer_description": "test",
    "offer_subcategoryid": "rrez",
    "rayon": "test",
    "macro_rayon": "test",
    "stock_price": 1,
    "image_url": "",
    "offer_type_label": "",
    "offer_sub_type_label": "",
    "author": "",
    "performer": "",
}
model_input = {
    "offer_id": "",
    "offer_name": "",
    "offer_description": "",
    "offer_subcategoryid": "",
    "rayon": "",
    "macro_rayon": "",
    "stock_price": 0,
    "image_url": "",
    "offer_type_label": "",
    "offer_sub_type_label": "",
    "author": "",
    "performer": "",
}
prediction = api_model.predict(model_input=model_input)
print(prediction)


# %%
class Testt:
    def __init__(self, a):
        self.a = a

    def apply(self, b):
        return self.a + b


# %%
