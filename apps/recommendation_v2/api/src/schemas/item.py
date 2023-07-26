from dataclasses import dataclass


@dataclass
class Item:
    item_id: str
    recommendation_score: float = None
