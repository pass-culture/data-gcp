from pydantic import BaseModel
from crud.iris import get_iris_from_coordinates

from sqlalchemy.orm import Session
from crud.offer import get_offer_characteristics 

class OfferInput(BaseModel):
    """Acceptable input in a API request for offer"""
    offer_id: str
    longitude: float = None
    latitude: float = None


class Offer:
    """Characteristics of an offer"""
    def __init__(
            self,
            offer_id,
            call_id=None,
            latitude=None,
            longitude=None,
            db: Session=None
        ) -> None:

        self.id = offer_id
        self.call_id = call_id
        if latitude and longitude:
            self.iris_id = get_iris_from_coordinates(db, longitude, latitude)
        self.item_id, self.cnt_bookings = get_offer_characteristics(db, offer_id)




