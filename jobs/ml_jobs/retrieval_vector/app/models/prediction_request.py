from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

DEFAULT_VECTOR_FIELDS = ["vector", "raw_embeddings"]
DEFAULT_TREND_FIELDS = [
    "booking_number_desc",
    "booking_trend_desc",
    "booking_creation_trend_desc",
    "booking_release_trend_desc",
]
MODEL_TYPES = {
    "recommendation": DEFAULT_VECTOR_FIELDS,
    "semantic": DEFAULT_VECTOR_FIELDS,
    "similar_offer": DEFAULT_VECTOR_FIELDS,
    "filter": DEFAULT_TREND_FIELDS,
    "tops": DEFAULT_TREND_FIELDS,
}
VALID_MODEL_TYPES = set(MODEL_TYPES.keys())


class PredictionRequest(BaseModel):
    """
    Pydantic model for parsing the incoming prediction request.
    Ensures if 'items' is empty, 'offer_id' is used as fallback.
    """

    model_type: str
    size: Optional[int] = Field(
        default=500,
        ge=1,
        description="Number of results to return, must be greater than 0.",
    )
    debug: Optional[bool] = Field(default=False, description="Enable debug mode.")
    prefilter: Optional[bool] = Field(
        default=False, description="Apply prefiltering if set."
    )
    re_rank: Optional[bool] = Field(
        default=False, description="Apply re-ranking if set."
    )
    vector_column_name: Optional[str] = Field(
        default="vector", description="Column name for the vector data."
    )
    params: Optional[dict] = Field(
        default_factory=dict, description="Additional filtering parameters."
    )
    call_id: Optional[str] = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the call.",
    )
    user_id: Optional[str] = None
    text: Optional[str] = None
    items: Optional[List[str]] = []
    excluded_items: Optional[List[str]] = []
    offer_id: Optional[str] = None

    model_config = ConfigDict(protected_namespaces=())

    @property
    def is_prefilter(self) -> bool:
        """Determine if prefiltering should be applied based on the prefilter flag or if params are present."""
        return self.prefilter or bool(self.params)

    @model_validator(mode="before")
    def ensure_items_fallback(cls, values):
        """
        If 'items' is empty, use 'offer_id' as a fallback.
        This can be removed once 'offer_id' is fully migrated to the 'items' list.
        """
        if not values.get("items") and values.get("offer_id"):
            values["items"] = [values["offer_id"]]
        return values

    @model_validator(mode="before")
    def validate_vector_column_name(cls, values):
        """
        Validates that the vector_column_name is appropriate for the given model_type.
        Ensures the vector_column_name is one of the allowed fields for the selected model_type.
        """
        model_type = values.get("model_type")
        vector_column_name = values.get("vector_column_name")

        if model_type and vector_column_name:
            valid_columns = MODEL_TYPES.get(model_type)
            if valid_columns and vector_column_name not in valid_columns:
                raise ValueError(
                    f"Invalid vector_column_name '{vector_column_name}' for model_type '{model_type}'. "
                    f"Must be one of {valid_columns}."
                )
        return values
