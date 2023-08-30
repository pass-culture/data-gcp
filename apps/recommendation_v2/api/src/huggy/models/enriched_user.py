from sqlalchemy import Column, String, Integer, DateTime, Float

from huggy.utils.database import Base


class User(Base):
    """Database model of enriched_user table.
    This table is used to get informations about the user calling the API."""

    __tablename__ = "enriched_user"
    user_id = Column(String(256), primary_key=True)
    user_deposit_creation_date = Column(DateTime(timezone=True))
    user_birth_date = Column(DateTime(timezone=True))
    user_deposit_initial_amount = Column(Float)
    user_theoretical_remaining_credit = Column(Float)
    booking_cnt = Column(Integer)
    consult_offer = Column(Integer)
    has_added_offer_to_favorites = Column(Integer)
