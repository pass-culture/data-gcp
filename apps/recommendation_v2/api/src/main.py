from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_versioning import VersionedFastAPI, version
from typing_extensions import Annotated

from utils.database import Base, engine, SessionLocal

from sqlalchemy.orm import Session
import uuid

from schemas.user import UserInput
from schemas.offer import OfferInput
from schemas.playlist_params import PlaylistParams

from core.model_engine.similar_offer import SimilarOffer
from core.model_engine.recommendation import Recommendation
from crud.user import get_user_profile
from crud.offer import get_offer_characteristics
from utils.env_vars import cloud_trace_context
from utils.cloud_logging.setup import setup_logging

app = FastAPI(title="Passculture refacto reco API")


async def setup_trace(request: Request):
    custom_logger.info("Setting up trace..")
    if "x-cloud-trace-context" in request.headers:
        cloud_trace_context.set(request.headers.get("x-cloud-trace-context"))


custom_logger = setup_logging()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/", dependencies=[Depends(setup_trace)])
def read_root():
    logger.info("Auth user welcome to : Refacto API test")
    return "Auth user welcome to : Refacto API test"


@app.get("/check")
def check():
    return "OK"


@app.post("/similar_offers")
def similar_offers(
    offer: OfferInput,
    user: UserInput,
    playlist_params: PlaylistParams,
    db: Session = Depends(get_db),
):

    call_id = str(uuid.uuid4())

    user = get_user_profile(db, user.user_id, call_id, user.latitude, user.longitude)

    offer = get_offer_characteristics(
        db, offer.offer_id, offer.latitude, offer.longitude
    )

    scoring = SimilarOffer(user, offer, playlist_params)

    offer_recommendations = scoring.get_scoring()

    log_extra_data = {
        "user_id": user.user_id,
        "iris_id": user.iris_id,
        "call_id": call_id,
        # 'reco_origin': scoring.reco_origin,
        # 'filters': ,
        # 'model_name': scoring.model_name,
        # 'model_version': scoring.model_version,
        # 'endpoint_name': scoring.endpoint_name ,
        # 'nb_recommendable_items': len(scoring.recommendable_items), # retrieval
        # 'nb_recommendable_offers': len(scoring.recommendable_offers), # ranking
        # 'nb_recommended_offers': len(user_recommendations),
    }
    # add nbr offres recommandables, nbr d'offres recommandées, endpoint appelé, model_name, param filters, call_id, temps de requête

    custom_logger.info("Get user profile", extra=log_extra_data)

    # scoring.save_recommendation(db, offer_recommendations)

    return offer_recommendations


@app.post("/playlist_recommendation", dependencies=[Depends(setup_trace)])
def playlist_recommendation(user: UserInput, db: Session = Depends(get_db)):

    call_id = str(uuid.uuid4())

    user = get_user_profile(db, user.user_id, call_id, user.latitude, user.longitude)

    log_extra_data = {"user_id": user.user_id, "iris_id": user.iris_id}

    custom_logger.info("Get user profile", extra=log_extra_data)

    scoring = Recommendation(user)

    user_recommendations = scoring.get_scoring(db)

    scoring.save_recommendation(db, user_recommendations)

    return user_recommendations
