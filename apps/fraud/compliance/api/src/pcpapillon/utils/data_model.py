from typing import Union

from pcpapillon.utils.env_vars import MODEL_DEFAULT, MODEL_STAGE
from pydantic import BaseModel

# from __future__ import annotations
from dataclasses import dataclass
from dataclass_wizard import JSONWizard


@dataclass
class APIConfig(JSONWizard):
    features_to_extract_embedding: list[dict]
    preprocess_features_type: dict[str]


@dataclass
class ModelConfig(JSONWizard):
    pre_trained_model_for_embedding_extraction: dict[str]
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
    offer_id: Union[str, None] = ""
    offer_name: Union[str, None] = ""
    offer_description: Union[str, None] = ""
    offer_subcategoryid: Union[str, None] = ""
    rayon: Union[str, None] = ""
    macro_rayon: Union[str, None] = ""
    stock_price: Union[float, None] = 0
    image_url: Union[str, None] = ""
    offer_type_label: Union[str, None] = ""
    offer_sub_type_label: Union[str, None] = ""
    author: Union[str, None] = ""
    performer: Union[str, None] = ""


class ComplianceOutput(BaseModel):
    offer_id: str
    probability_validated: int
    validation_main_features: list[str]
    probability_rejected: int
    rejection_main_features: list[str]


class ModelParams(BaseModel):
    name: str = "default"
    type: str = "default"
