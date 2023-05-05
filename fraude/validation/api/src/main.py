import os
import sys
import time
from datetime import timedelta

import mlflow
import mlflow.pyfunc
from catboost import CatBoostClassifier
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_cloud_logging import (FastAPILoggingHandler,
                                   RequestLoggingMiddleware)
from fastapi_versioning import VersionedFastAPI, version
from google.cloud.logging import Client
from google.cloud.logging_v2.handlers import setup_logging
from loguru import logger
from pcvalidation.core.extract_embedding import extract_embedding
from pcvalidation.core.predict import  get_prediction,get_prediction_main_contribution
from pcvalidation.core.preprocess import (convert_data_to_catboost_pool,
                                          preprocess)
from pcvalidation.utils.configs import default_config as params
from pcvalidation.utils.data_model import Item, Token, User, model_params
from pcvalidation.utils.env_vars import (LOGIN_TOKEN_EXPIRATION,
                                         MLFLOW_CLIENT_ID, users_db)
from pcvalidation.utils.security import (authenticate_user,
                                         create_access_token,
                                         get_current_active_user)
from pcvalidation.utils.tools import connect_remote_mlflow
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

#Load latest model from MLFlow registery
connect_remote_mlflow(MLFLOW_CLIENT_ID)
model = CatBoostClassifier(one_hot_max_size=65)
model_loaded = mlflow.catboost.load_model(model_uri=f"models:/validation_model_test/Staging")

@app.get("/")
def read_root():
    logger.info("Auth user welcome to : Validation API test")
    return "Auth user welcome to : Validation API test"

@app.post("/get_validation_score")
@version(1, 0)
def get_item_validation_score(
    item: Item, current_user: Annotated[User, Depends(get_current_active_user)]
):
    start = time.time()
    context_logger = logger.bind(
        model_version="default_model", offer_id=item.dict()["offer_id"]
    )
    context_logger.info("get_item_validation_score ")

    # df = pd.DataFrame(item.dict(), index=[0])
    data = item.dict()
    # context_logger.bind(input=data).info("Input DATA ")
    data_clean = preprocess(data, params["preprocess_features_type"])

    data_w_emb = extract_embedding(data_clean, params["features_to_extract_embedding"])

    pool = convert_data_to_catboost_pool(
        data_w_emb, params["catboost_features_types"]
    )

    proba_val, proba_rej = get_prediction(model_loaded, pool)
    top_val, top_rej = get_prediction_main_contribution(model_loaded, data_w_emb, pool)

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

@app.post("/load_validation_model/")
def load_model(model_params: model_params,current_user: Annotated[User, Depends(get_current_active_user)]):
    logger.info("")
    connect_remote_mlflow(MLFLOW_CLIENT_ID)
    global model_loaded
    model_loaded = mlflow.catboost.load_model(model_uri=f"models:/validation_model_test/Staging")
    logger.info("Validation model updated")
    return model_params

@app.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    context_logger = logger.bind(
        username=form_data.username
    )
    context_logger.info("Requesting access token")
    user = authenticate_user(users_db, form_data.username, form_data.password)
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

app = VersionedFastAPI(app, enable_latest=True)
