from sqlalchemy.orm import Session
from sqlalchemy import func

from models.item_ids_mv import ItemIdsMv

def get_offer_characteristics(db: Session, offer_id: str) -> dict:
    """ Query the database in ORM mode to get characteristics of an offer.
    Return : List[item_id,  number of booking associated].
    """
    offer_characteristics = (
        db
        .query(ItemIdsMv.item_id, ItemIdsMv.booking_number)
        .filter(ItemIdsMv.offer_id == offer_id)
        .first()
    )

    # log_duration(f"get_offer_characteristics for offer_id: {offer_id}", start)
    if offer_characteristics is not None:
        # logger.info("get_offer_characteristics:found id")
        return offer_characteristics
    else:
        # logger.info("get_offer_characteristics:not_found_id")
        return None, 0
