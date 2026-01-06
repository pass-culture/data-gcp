from pydantic import BaseModel, Field


class PredictionRequest(BaseModel):
    """
    Pydantic model for parsing the incoming prediction request.
    Ensures if 'items' is empty, 'offer_id' is used as fallback.
    """

    search_query: str = Field(..., description="The search query string.")
    filters_list: list[dict] | None = Field(
        None, description="Optional list of filter dictionaries."
    )


class ItemSelection(BaseModel):
    item_id: str
    pertinence: str


class OfferSelection(BaseModel):
    offer_id: str
    pertinence: str


class EditorialResult(BaseModel):
    items: list[ItemSelection] = Field(default_factory=list)


class SearchResult(BaseModel):
    offers: list[OfferSelection] = Field(default_factory=list)


class PredictionResult(BaseModel):
    """
    Pydantic model for structuring the prediction result.
    """

    # offers: List[dict] = Field(..., description="List of predicted offers.")
    predictions: SearchResult = Field(..., description="List of predicted predictions.")
