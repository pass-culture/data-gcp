from pydantic import BaseModel
from datetime import datetime
from typing import List, Dict

from huggy.utils.env_vars import NUMBER_OF_RECOMMENDATIONS


class PlaylistParams(BaseModel):
    """Acceptable input in a API request for recommendations filters."""

    model_endpoint: str = None
    start_date: datetime = None
    end_date: datetime = None
    beginning_datetime: datetime = None
    ending_datetime: datetime = None
    is_event: bool = None
    is_duo: bool = None
    categories: List[str] = None
    subcategories: List[str] = None
    offer_type_list: List[Dict] = None
    price_max: float = None
    price_min: float = None
    nb_reco_display: int = NUMBER_OF_RECOMMENDATIONS
    is_reco_mixed: bool = None
    is_reco_shuffled: bool = None
    is_digital: bool = True
    mixing_features: List[str] = None
