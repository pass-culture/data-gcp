from sqlalchemy import Column, String, Integer

from huggy.utils.database import Base


class ItemIdsMv(Base):
    """Database model of item_ids materialized view."""

    __tablename__ = "item_ids_mv"
    item_id = Column(String, primary_key=True)
    offer_id = Column(String, primary_key=True)
    booking_number = Column(Integer)
