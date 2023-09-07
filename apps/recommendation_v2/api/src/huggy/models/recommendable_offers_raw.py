from sqlalchemy import Column, String, Integer, Float, DateTime, ForeignKey, inspect
from sqlalchemy.types import Boolean

from huggy.utils.database import Base


class RecommendableOffersRaw(Base):
    __tablename__ = "recommendable_offers_raw"
    offer_id = Column(String(256), primary_key=True)
    item_id = Column(String(256))
    product_id = Column(String(256))
    category = Column(String(256))
    subcategory_id = Column(String(256))
    search_group_name = Column(String(256))
    venue_id = Column(String(256))
    name = Column(String(256))
    is_numerical = Column(Boolean)
    is_national = Column(Boolean)
    is_geolocated = Column(Boolean)
    offer_creation_date = Column(DateTime)
    stock_beginning_date = Column(DateTime)
    stock_price = Column(Float)
    offer_is_duo = Column(Boolean)
    offer_type_domain = Column(String(256))
    offer_type_label = Column(String(256))
    booking_number = Column(Integer)
    is_underage_recommendable = Column(Boolean)
    venue_latitude = Column(Float)
    venue_longitude = Column(Float)
    default_max_distance = Column(Integer)
    unique_id = Column(String(256))


class RecommendableOffersRawMv(RecommendableOffersRaw):
    __tablename__ = "recommendable_offers_raw_mv"
    offer_id = Column(
        None, ForeignKey("recommendable_offers_raw.offer_id"), primary_key=True
    )
    __mapper_args__ = {
        "polymorphic_identity": "mv",
        "inherit_condition": (offer_id == RecommendableOffersRaw.offer_id),
    }


class RecommendableOffersRawMvTemp(RecommendableOffersRaw):
    __tablename__ = "recommendable_offers_raw_tmp"
    offer_id = Column(
        None, ForeignKey("recommendable_offers_raw.offer_id"), primary_key=True
    )
    __mapper_args__ = {
        "polymorphic_identity": "mv_temp",
        "inherit_condition": (offer_id == RecommendableOffersRaw.offer_id),
    }


class RecommendableOffersRawMvOld(RecommendableOffersRaw):
    __tablename__ = "recommendable_offers_raw_mv_old"
    offer_id = Column(
        None, ForeignKey("recommendable_offers_raw.offer_id"), primary_key=True
    )
    __mapper_args__ = {
        "polymorphic_identity": "mv_old",
        "inherit_condition": (offer_id == RecommendableOffersRaw.offer_id),
    }


def get_available_table(engine, offer_model_base) -> str:
    for suffix in ["", "Mv", "MvTmp", "MvOld"]:
        offer_model = f"{offer_model_base}{suffix}"
        table_name = eval(offer_model).__tablename__
        result = inspect(engine).has_table(table_name)
        if result:
            return eval(offer_model)
