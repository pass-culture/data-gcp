from fastapi import Depends, FastAPI, Request
from fastapi.logger import logger
from sqlalchemy.orm import Session
import uuid

from huggy.schemas.user import UserInput
from huggy.schemas.offer import OfferInput
from huggy.schemas.playlist_params import PlaylistParams

from huggy.core.model_engine.similar_offer import SimilarOffer
from huggy.core.model_engine.recommendation import Recommendation

from huggy.crud.user import get_user_profile
from huggy.crud.offer import get_offer_characteristics

from huggy.utils.database import SessionLocal
from huggy.utils.env_vars import cloud_trace_context
from huggy.utils.cloud_logging.setup import setup_logging

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

    offer_recommendations = scoring.get_scoring(db)

    log_extra_data = {
        "user_id": user.user_id,
        "offer_id": offer.offer_id,
        "iris_id": user.iris_id,
        "call_id": call_id,
        "reco_origin": scoring.reco_origin,
        # 'filters': playlist_params,
        "retrieval_model_name": scoring.scorer.retrieval_endpoints[
            0
        ].model_display_name,
        "retrieval_model_version": scoring.scorer.retrieval_endpoints[0].model_version,
        "retrieval_endpoint_name": scoring.scorer.retrieval_endpoints[0].endpoint_name,
        "ranking_model_name": scoring.scorer.ranking_endpoint.model_display_name,
        "ranking_model_version": scoring.scorer.ranking_endpoint.model_version,
        "ranking_endpoint_name": scoring.scorer.ranking_endpoint.endpoint_name,
        "recommended_offers": offer_recommendations,
    }

    custom_logger.info(
        f"Get similar offer of offer_id {offer.offer_id} for user {user.user_id}",
        extra=log_extra_data,
    )

    scoring.save_recommendation(db, offer_recommendations)

    return offer_recommendations


@app.post("/playlist_recommendation", dependencies=[Depends(setup_trace)])
def playlist_recommendation(
    user: UserInput, playlist_params: PlaylistParams, db: Session = Depends(get_db)
):

    call_id = str(uuid.uuid4())

    user = get_user_profile(db, user.user_id, call_id, user.latitude, user.longitude)

    scoring = Recommendation(user, params_in=playlist_params)

    user_recommendations = scoring.get_scoring(db)

    log_extra_data = {
        "user_id": user.user_id,
        "iris_id": user.iris_id,
        "call_id": call_id,
        "reco_origin": scoring.reco_origin,
        # 'filters': playlist_params,
        "retrieval_model_name": scoring.scorer.retrieval_endpoints[
            0
        ].model_display_name,
        "retrieval_model_version": scoring.scorer.retrieval_endpoints[0].model_version,
        "retrieval_endpoint_name": scoring.scorer.retrieval_endpoints[0].endpoint_name,
        "ranking_model_name": scoring.scorer.ranking_endpoint.model_display_name,
        "ranking_model_version": scoring.scorer.ranking_endpoint.model_version,
        "ranking_endpoint_name": scoring.scorer.ranking_endpoint.endpoint_name,
        "recommended_offers": user_recommendations,
    }

    custom_logger.info(
        f"Get recommendations for user {user.user_id}", extra=log_extra_data
    )

    scoring.save_recommendation(db, user_recommendations)

    return user_recommendations
