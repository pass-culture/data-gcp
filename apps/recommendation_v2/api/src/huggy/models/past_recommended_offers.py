from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.types import PickleType

from huggy.utils.database import Base


class PastRecommendedOffers(Base):
    """Database model of past_recommendable_offers table.
    This table is used to log the offers recommended to an user."""

    __tablename__ = "past_recommended_offers"
    call_id = Column(String(256), primary_key=True)
    userid = Column(String(256), primary_key=True)
    offerid = Column(String(256), primary_key=True)
    date = Column(DateTime(timezone=True))
    group_id = Column(String(256))
    reco_origin = Column(String(256))
    model_name = Column(String(256))
    model_version = Column(String(256))
    reco_filters = Column(PickleType)
    user_iris_id = Column(Integer)


class PastSimilarOffers(Base):
    """Database model of past_recommendable_offers table.
    This table is used to log the offers recommended to an user."""

    __tablename__ = "past_similar_offers"
    call_id = Column(String(256), primary_key=True)
    user_id = Column(String(256), primary_key=True)
    offer_id = Column(String(256), primary_key=True)
    origin_offer_id = Column(String(256))
    date = Column(DateTime(timezone=True))
    group_id = Column(String(256))
    model_name = Column(String(256))
    model_version = Column(String(256))
    reco_filters = Column(PickleType)
    venue_iris_id = Column(Integer)
