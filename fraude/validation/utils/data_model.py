from pydantic import BaseModel
from typing import Union
from utils.env_vars import GCS_BUCKET


class User(BaseModel):
    username: str
    email: Union[str, None] = None
    full_name: Union[str, None] = None
    disabled: Union[bool, None] = None


class UserInDB(User):
    hashed_password: str


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
    model_bucket: str = GCS_BUCKET
    model_remote_path: str
    model_local_path: str = "./model/validation_model.cbm"
