import time
from datetime import timedelta

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_versioning import VersionedFastAPI, version
from typing_extensions import Annotated

from schemas.playlist_params import PlaylistParamsRequest

from utils.database import Base, engine

from sqlalchemy import func
from sqlalchemy.orm import Session
import uuid

from schemas.user import UserInput, User
from schemas.offer import OfferInput, Offer

from core.model_engine.similar_offer import SimilarOffer
from crud.recommendable_offers import get_user_distance

app = FastAPI(title="Passculture refacto reco API")

Base.metadata.create_all(engine)

# async def setup_trace(request: Request):
#     custom_logger.info("Setting up trace..")
#     if "x-cloud-trace-context" in request.headers:
#         cloud_trace_context.set(request.headers.get("x-cloud-trace-context"))


# custom_logger = setup_logging()


@app.get("/")
def read_root():
    # logger.info("Auth user welcome to : Refacto API test")
    return "Auth user welcome to : Refacto API test"


@app.get("/check")
def check():
    return "OK"


@app.post("/similar_offers")
def similar_offers(
    offer: OfferInput, user: UserInput, playlist_params: PlaylistParamsRequest
):

    call_id = str(uuid.uuid4())

    db = Session(bind=engine, expire_on_commit=False)

    user = User(
        user_id=user.user_id,
        call_id=call_id,
        longitude=user.longitude,
        latitude=user.latitude,
        db=db,
    )

    offer = Offer(
        offer_id=offer.offer_id,
        call_id=call_id,
        latitude=offer.latitude,
        longitude=offer.longitude,
        db=db,
    )

    scoring = SimilarOffer(user, offer, params_in=playlist_params)
    offer_recommendations = scoring.get_scoring()
    scoring.save_recommendation(db, offer_recommendations)
    db.close()
    return offer_recommendations


@app.post("/playlist_recommendation")
def playlist_recommendation(user: UserInput, playlist_params: PlaylistParamsRequest):

    call_id = str(uuid.uuid4())

    user = User(
        user_id=user.user_id,
        call_id=call_id,
        longitude=user.longitude,
        latitude=user.latitude,
    )

    return user.user_profile, user.iris_id
