from fastapi import Query
from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Dict
import re


under_pat = re.compile(r"_([a-z])")


def underscore_to_camel(name):
    """
    Parse key into camelCase format
    """
    return under_pat.sub(lambda x: x.group(1).upper(), name)


class PlaylistParams(BaseModel):
    """Acceptable input in a API request for recommendations filters."""

    model_endpoint: str = None
    start_date: datetime = None
    end_date: datetime = None
    is_event: bool = None
    is_duo: bool = None
    price_max: float = None
    price_min: float = None
    is_reco_shuffled: bool = None
    is_digital: bool = True

    class Config:
        alias_generator = underscore_to_camel


class GetSimilarOfferPlaylistParams(PlaylistParams):
    user_id: str = None
    categories: List[str] = Field(Query([]))
    subcategories: List[str] = Field(Query([]))
    offer_type_list: str = None  # useless in similar offer


class PostSimilarOfferPlaylistParams(PlaylistParams):
    user_id: str = None
    categories: List[str] = None
    subcategories: List[str] = None
    offer_type_list: str = None  # useless in similar offer


class RecommendationPlaylistParams(PlaylistParams):
    categories: List[str] = None
    subcategories: List[str] = None
    offer_type_list: List[Dict] = None
