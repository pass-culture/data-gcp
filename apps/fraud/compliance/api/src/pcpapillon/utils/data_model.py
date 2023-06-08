from typing import Union

from pcpapillon.utils.env_vars import MODEL_DEFAULT, MODEL_STAGE
from pydantic import BaseModel

# from __future__ import annotations
from dataclasses import dataclass
from dataclass_wizard import JSONWizard


@dataclass
class Config(JSONWizard):
    features_to_extract_embedding: list[dict]
    pre_trained_model_for_embedding_extraction: dict[str]
    preprocess_features_type: dict[str]
    preprocess_features_type: dict[str]
    catboost_features_types: dict[str]


class User(BaseModel):
    username: str
    disabled: Union[bool, None] = None


class UserInDB(User):
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None


class Item(BaseModel):
    offer_id: str
    offer_name: str
    offer_description: str
    offer_subcategoryid: str
    rayon: str
    macro_rayon: str
    stock_price: str
    stock: str
    offer_image: str


class ComplianceOutput(BaseModel):
    offer_id: str
    probability_validated: int
    validation_main_features: list[str]
    probability_rejected: int
    rejection_main_features: list[str]


class ModelParams(BaseModel):
    model_name: str = MODEL_DEFAULT
    model_stage: str = MODEL_STAGE
