import json
import sys
import time


import pandas as pd
from catboost import CatBoostClassifier
from extract_embedding import extract_embedding
from fastapi import FastAPI, Security, Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from typing_extensions import Annotated

from fastapi_cloud_logging import FastAPILoggingHandler, RequestLoggingMiddleware
from fastapi_versioning import VersionedFastAPI, version
from google.cloud.logging import Client
from google.cloud.logging_v2.handlers import setup_logging
from loguru import logger
from predict import get_main_contribution, get_prediction
from preprocess import convert_dataframe_to_catboost_pool, preprocess

from utils.tools import download_blob, get_api_key

from utils.security import (
    fake_hash_password,
    get_user,
    fake_decode_token,
    get_current_user,
    get_current_active_user,
)
from utils.data_model import UserInDB, Item, model_params
from utils.env_vars import fake_users_db

logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time}</green> - {level} - <blue>{message}</blue>",
    serialize=True,
    level="INFO",
)
# logger.add(sys.stdout, format="{time} - {level} - {extra[model_version]} - {message}",serialize=True,level="INFO")

app = FastAPI(title="Passculture offer validation API")
# Add middleware
app.add_middleware(RequestLoggingMiddleware)

# Use manual handler
handler = FastAPILoggingHandler(Client(), structured=True)
setup_logging(handler)


@app.get("/")
def read_root(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    user_dict = fake_users_db.get(form_data.username)
    if not user_dict:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    user = UserInDB(**user_dict)
    hashed_password = fake_hash_password(form_data.password)
    if not hashed_password == user.hashed_password:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    context_logger = logger.bind(model_version="default_model")
    context_logger.info("Auth user welcome to : Validation API test")
    return "PassCulture - offer Validation API"


@app.get("/token")
def read_root(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    user_dict = fake_users_db.get(form_data.username)
    if not user_dict:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    user = UserInDB(**user_dict)
    hashed_password = fake_hash_password(form_data.password)
    if not hashed_password == user.hashed_password:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    context_logger = logger.bind(model_version="default_model")
    context_logger.info("Auth user welcome to : Validation API test")
    return "9d207bf0"


@app.post("/validation")
@version(1, 0)
def get_item_validation_score(item: Item, api_key: str = Security(get_api_key)):
    start = time.time()
    context_logger = logger.bind(
        model_version="default_model", offer_id=item.dict()["offer_id"]
    )
    context_logger.info("get_item_validation_score ")
    df = pd.DataFrame(item.dict(), index=[0])

    df_clean = preprocess(df)

    with open(
        "./configs/default_config.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)

    df_wEmb = extract_embedding(df_clean, params)

    pool = convert_dataframe_to_catboost_pool(df_wEmb, params["features_types"])

    model = CatBoostClassifier(one_hot_max_size=65)
    model_loaded = model.load_model("./model/validation_model", format="cbm")

    proba_val, proba_rej = get_prediction(model_loaded, pool)
    top_val, top_rej = get_main_contribution(model_loaded, df_wEmb, pool)

    output_dict = {
        "offer_id": item.dict()["offer_id"],
        "probability_validated": proba_val,
        "validation_main_features": top_val,
        "probability_rejected": proba_rej,
        "rejection_main_features": top_rej,
    }
    context_logger.bind(execution_time=time.time() - start).info(output_dict)
    return output_dict


@app.post("/load_new_model/")
def load_model(model_params: model_params):
    download_blob(model_params.dict())
    return model_params


app = VersionedFastAPI(app, enable_latest=True)
