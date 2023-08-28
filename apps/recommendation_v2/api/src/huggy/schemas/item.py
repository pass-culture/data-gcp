from dataclasses import dataclass


@dataclass
class Item:
    item_id: str
    item_score: float = None


@dataclass
class RecommendableItem:
    item_id: str
    item_score: float
    item_rank: int
