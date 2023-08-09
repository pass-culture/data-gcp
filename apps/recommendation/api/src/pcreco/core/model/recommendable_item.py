from dataclasses import dataclass


@dataclass
class RecommendableItem:
    item_id: str
    item_score: float
