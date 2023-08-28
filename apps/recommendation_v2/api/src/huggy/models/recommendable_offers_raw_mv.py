from sqlalchemy import Column, String, Integer, Float, DateTime
from sqlalchemy.types import Boolean

from huggy.utils.database import Base


class RecommendableOffersRaw(Base):
    __tablename__ = "recommendable_offers_raw_mv"
    item_id = Column(String(256), primary_key=True)
    offer_id = Column(String(256), primary_key=True)
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
