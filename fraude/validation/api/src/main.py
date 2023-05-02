import json
import sys
import time
from datetime import timedelta

from catboost import CatBoostClassifier
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_cloud_logging import FastAPILoggingHandler, RequestLoggingMiddleware
from fastapi_versioning import VersionedFastAPI, version
from google.cloud.logging import Client
from google.cloud.logging_v2.handlers import setup_logging
from loguru import logger
from pcvalidation.core.extract_embedding import extract_embedding
from pcvalidation.core.predict import get_main_contribution, get_prediction
from pcvalidation.core.preprocess import convert_dataframe_to_catboost_pool, preprocess
from pcvalidation.utils.data_model import Item, Token, User, model_params
from pcvalidation.utils.env_vars import LOGIN_TOKEN_EXPIRATION, fake_users_db
from pcvalidation.utils.security import (
    authenticate_user,
    create_access_token,
    get_current_active_user,
)
from pcvalidation.utils.tools import download_blob
from typing_extensions import Annotated

logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time}</green> - {level} - <blue>{message}</blue>",
    serialize=True,
    level="INFO",
)

app = FastAPI(title="Passculture offer validation API")

# Add middleware
app.add_middleware(RequestLoggingMiddleware)
# Use manual handler
handler = FastAPILoggingHandler(Client(), structured=True)
setup_logging(handler)
download_blob(
    {
        "model_local_path": "./models/validation_model_main.cbm",
        "model_remote_path": "Validation_offres/model/validation_model",
        "model_bucket": "data-bucket-prod",
    }
)
model = CatBoostClassifier(one_hot_max_size=65)
model_loaded = model.load_model("./models/validation_model.cbm", format="cbm")


@app.get("/")
def read_root():
    logger.info("Auth user welcome to : Validation API test")
    return "Auth user welcome to : Validation API test"


@app.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=LOGIN_TOKEN_EXPIRATION)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/validation")
@version(1, 0)
def get_item_validation_score(
    item: Item, current_user: Annotated[User, Depends(get_current_active_user)]
):
    start = time.time()
    context_logger = logger.bind(
        model_version="default_model", offer_id=item.dict()["offer_id"]
    )
    context_logger.info("get_item_validation_score ")
    with open(
        "./configs/default_config.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)
    # df = pd.DataFrame(item.dict(), index=[0])
    data = item.dict()
    # context_logger.bind(input=data).info("Input DATA ")
    data_clean = preprocess(data, params["preprocess_features_type"])

    data_w_emb = extract_embedding(data_clean, params["features_to_extract_embedding"])

    pool = convert_dataframe_to_catboost_pool(
        data_w_emb, params["catboost_features_types"]
    )

    proba_val, proba_rej = get_prediction(model_loaded, pool)
    top_val, top_rej = get_main_contribution(model_loaded, data_w_emb, pool)

    validation_response_dict = {
        "offer_id": item.dict()["offer_id"],
        "probability_validated": proba_val,
        "validation_main_features": top_val,
        "probability_rejected": proba_rej,
        "rejection_main_features": top_rej,
    }
    context_logger.bind(execution_time=time.time() - start).info(
        validation_response_dict
    )
    return validation_response_dict


@app.post("/load_new_model/")
def load_model(model_params: model_params):
    download_blob(model_params.dict())
    model_loaded = model.load_model(
        model_params.dict()["model_local_path"], format="cbm"
    )
    return model_params


app = VersionedFastAPI(app, enable_latest=True)
