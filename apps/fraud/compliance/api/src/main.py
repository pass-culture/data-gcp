import time
from datetime import timedelta

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_versioning import VersionedFastAPI, version
from pcpapillon.core.predict import get_prediction_and_main_contribution
from pcpapillon.core.preprocess import preprocess
from pcpapillon.utils.cloud_logging.setup import setup_logging
from pcpapillon.utils.data_model import (
    Config,
    Item,
    Token,
    User,
    ModelParams,
    ComplianceOutput,
)
from pcpapillon.utils.env_vars import (
    API_PARAMS,
    LOGIN_TOKEN_EXPIRATION,
    cloud_trace_context,
    users_db,
)
from pcpapillon.utils.model_handler import ModelHandler
from pcpapillon.utils.security import (
    authenticate_user,
    create_access_token,
    get_current_active_user,
)
from typing_extensions import Annotated

app = FastAPI(title="Passculture offer validation API")


async def setup_trace(request: Request):
    custom_logger.info("Setting up trace..")
    if "x-cloud-trace-context" in request.headers:
        cloud_trace_context.set(request.headers.get("x-cloud-trace-context"))


custom_logger = setup_logging()
model_handler = ModelHandler()
api_config = Config.from_dict(API_PARAMS)
custom_logger.info("load_compliance_model..")
model_loaded = model_handler.get_model_by_name("compliance")
custom_logger.info("load_preproc_model..")
prepoc_models = {}
for feature_type in api_config.pre_trained_model_for_embedding_extraction.keys():
    prepoc_models[feature_type] = model_handler.get_model_by_name(feature_type)


@app.get("/")
def read_root():
    logger.info("Auth user welcome to : Validation API test")
    return "Auth user welcome to : Validation API test"


@app.post(
    "/model/compliance/scoring",
    response_model=ComplianceOutput,
    dependencies=[Depends(setup_trace)],
)
@version(1, 0)
def model_compliance_scoring(
    item: Item, current_user: Annotated[User, Depends(get_current_active_user)]
):
    start = time.time()
    log_extra_data = {
        "model_version": "default_model",
        "offer_id": item.dict()["offer_id"],
        "scoring_input": item.dict(),
    }
    custom_logger.info(prepoc_models)
    pool, data_w_emb = preprocess(item.dict(), prepoc_models)
    (
        proba_validation,
        proba_rejection,
        top_validation,
        top_rejection,
    ) = get_prediction_and_main_contribution(model_loaded, data_w_emb, pool)

    validation_response_dict = {
        "offer_id": item.dict()["offer_id"],
        "probability_validated": proba_validation,
        "validation_main_features": top_validation,
        "probability_rejected": proba_rejection,
        "rejection_main_features": top_rejection,
    }
    custom_logger.info(validation_response_dict, extra=log_extra_data)
    return validation_response_dict


@app.post("/model/compliance/load", dependencies=[Depends(setup_trace)])
@version(1, 0)
def model_compliance_load(
    model_params: ModelParams,
    current_user: Annotated[User, Depends(get_current_active_user)],
):
    log_extra_data = {"model_params": model_params.dict()}
    custom_logger.info("Loading new model", extra=log_extra_data)
    global model_loaded
    model_loaded = model_handler.get_model_by_name("compliance")
    custom_logger.info("Validation model updated", extra=log_extra_data)
    return model_params


@app.post("/token", response_model=Token, dependencies=[Depends(setup_trace)])
@version(1, 0)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    log_extra_data = {"user": form_data.username}
    custom_logger.info("Requesting access token", extra=log_extra_data)
    user = authenticate_user(users_db, form_data.username, form_data.password)
    if not user:
        exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        logger.info("Failed authentification", extra=log_extra_data)
        raise exception
    access_token_expires = timedelta(minutes=LOGIN_TOKEN_EXPIRATION)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    custom_logger.info("Successful authentification", extra=log_extra_data)
    return {"access_token": access_token, "token_type": "bearer"}


app = VersionedFastAPI(app, enable_latest=True)
