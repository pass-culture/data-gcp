from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_versioning import VersionedFastAPI, version
from typing_extensions import Annotated

from utils.database import Base, engine

from sqlalchemy.orm import Session
import uuid

from schemas.user import UserInput
from schemas.offer import OfferInput

from core.model_engine.similar_offer import SimilarOffer
from crud.user import get_user_profile
from crud.offer import get_offer_characteristics

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
def similar_offers(offer: OfferInput, user: UserInput):

    call_id = str(uuid.uuid4())

    db = Session(bind=engine, expire_on_commit=False)

    user = get_user_profile(db, user.user_id, call_id, user.latitude, user.longitude)

    offer = get_offer_characteristics(
        db, offer.offer_id, offer.latitude, offer.longitude
    )

    scoring = SimilarOffer(user, offer)

    offer_recommendations = scoring.get_scoring(db)

    scoring.save_recommendation(db, offer_recommendations)

    db.close()

    return offer_recommendations


# @app.post("/playlist_recommendation")
# def playlist_recommendation(user: UserInput):

#     call_id = str(uuid.uuid4())

#     user = User(
#         user_id=user.user_id,
#         call_id=call_id,
#         longitude=user.longitude,
#         latitude=user.latitude,
#     )

#     return user.user_profile, user.iris_id
