from sqlalchemy import Column, String, Integer, Float, DateTime

from huggy.utils.database import Base


class NonRecommendableItems(Base):
    __tablename__ = "non_recommendable_items"
    item_id = Column(String(256), primary_key=True)
    user_id = Column(String(256), primary_key=True)
