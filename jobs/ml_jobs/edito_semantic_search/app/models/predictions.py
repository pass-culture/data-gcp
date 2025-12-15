from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PredictionRequest(BaseModel):
    """
    Pydantic model for parsing the incoming prediction request.
    Ensures if 'items' is empty, 'offer_id' is used as fallback.
    """
    search_query: str = Field(..., description="The search query string.")
    filters_list: Optional[List[dict]] = Field(
        None, description="Optional list of filter dictionaries."
    )

class PredictionResult(BaseModel):
    """
    Pydantic model for structuring the prediction result.
    """
    offers: List[dict] = Field(..., description="List of predicted offers.")