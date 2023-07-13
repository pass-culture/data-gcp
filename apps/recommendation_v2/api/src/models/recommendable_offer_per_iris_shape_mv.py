from sqlalchemy import Column, String, Integer, Float, DateTime
from sqlalchemy.types import Boolean
from geoalchemy2 import Geometry

from utils.database import Base

class RecommendableOfferIrisShape(Base):
    __tablename__ = 'recommendable_offer_per_iris_shape_mv'
    item_id = Column(String(256), primary_key=True)
    offer_id = Column(String(256), primary_key=True)
    iris_id = Column(String(256), primary_key=True)
    venue_distance_to_iris = Column(Float)
    is_geolocated  = Column(Boolean)
    venue_latitude = Column(Float)
    venue_longitude = Column(Float)
    venue_geo = Column(Geometry('POINT'))
    unique_id = Column(String(256))

