from typing import Union

from pcvalidation.utils.env_vars import MODEL_DEFAULT, MODEL_STAGE
from pydantic import BaseModel


class User(BaseModel):
    username: str
    email: Union[str, None] = None
    full_name: Union[str, None] = None
    disabled: Union[bool, None] = None


class UserInDB(User):
    hashed_password: str


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
    venue_department_code: str
    stock_price: str
    stock: str
    offer_image: str
    type: str
    subType: str
    rayon: str
    macro_rayon: str


class model_params(BaseModel):
    model_name: str = MODEL_DEFAULT
    model_stage: str = MODEL_STAGE
