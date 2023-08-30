from sqlalchemy import Column, String, Integer

from huggy.utils.database import Base, bind_engine, get_available_materialized_view


class ItemIdsMv(Base):
    """Database model of item_ids materialized view."""

    __tablename__ = get_available_materialized_view(bind_engine, "item_ids")
    item_id = Column(String, primary_key=True)
    offer_id = Column(String, primary_key=True)
    booking_number = Column(Integer)
