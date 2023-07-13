import os
from enum import Enum

NUMBER_OF_RECOMMENDATIONS = 20
NUMBER_OF_PRESELECTED_OFFERS = 50 if not os.environ.get("CI") else 3
DEFAULT_SIMILAR_OFFER_MODEL = os.environ.get("DEFAULT_SIMILAR_OFFER_MODEL", "default")

class MixingFeatures(Enum):
    subcategory_id = "subcategory_id"
    search_group_name = "search_group_name"
    category = "category"


