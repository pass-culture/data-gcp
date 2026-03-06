from pydantic import BaseModel, Field


class PredictionRequest(BaseModel):
    """Incoming prediction request."""

    search_query: str = Field(..., description="The search query string.")
    filters_list: list[dict] | None = Field(
        None, description="Optional list of filter dictionaries."
    )


class ItemSelection(BaseModel):
    """A single selected item from the catalog."""

    item_id: str = Field(
        ...,
        description="The numeric item_id from the 'item_id' column of the table (e.g. '1', '2').",
    )
    pertinence: str = Field(
        ...,
        description="Brief explanation in French of why this item matches the query (max 2 sentences).",
    )


class OfferSelection(BaseModel):
    offer_id: str
    pertinence: str


class EditorialResult(BaseModel):
    """List of items selected by the LLM from the catalog."""

    items: list[ItemSelection] = Field(
        default_factory=list,
        description="List of selected items. Empty list if no items match.",
    )


class SearchResult(BaseModel):
    offers: list[OfferSelection] = Field(default_factory=list)


class PredictionResult(BaseModel):
    """
    Pydantic model for structuring the prediction result.
    """

    # offers: List[dict] = Field(..., description="List of predicted offers.")
    predictions: SearchResult = Field(..., description="List of predicted predictions.")
