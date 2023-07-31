from pcreco.core.user import User
from pcreco.utils.env_vars import log_duration
import time
from pcreco.core.utils.cold_start import (
    get_cold_start_categories,
)
import random
import heapq
from pcreco.core.utils.vertex_ai import endpoint_score
from pcreco.utils.env_vars import (
    log_duration,
)
from pcreco.core.offer import Offer
from pcreco.core.scorer import ModelEndpoint
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from datetime import datetime
from dataclasses import dataclass
import typing as t


@dataclass
class ListParams:
    label: str
    values: t.List[str] = None

    def filter(self):
        if self.values is not None and len(self.values) > 0:
            return {self.label: {"$in": self.values}}
        return {}


@dataclass
class RangeParams:
    label: str
    max_val: float = None
    min_val: float = None

    def filter(self):
        if self.min_val is not None and self.max_val is not None:
            return {self.label: {"$gte": self.min_val, "$lte": self.max_val}}
        elif self.min_val is not None:
            return {self.label: {"$gte": self.min_val}}
        elif self.max_val is not None:
            return {self.label: {"$lte": self.min_val}}
        else:
            return {}


@dataclass
class DateParams(RangeParams):
    label: str
    max_val: datetime = None
    min_val: datetime = None

    def filter(self):
        if self.min_val is not None:
            self.min_val = float(self.min_val.timestamp())

        if self.max_val is not None:
            self.max_val = float(self.max_val.timestamp())

        return super().filter()


@dataclass
class EqParams:
    label: str
    value: float = None

    def filter(self):
        if self.value is not None:
            return {self.label: {"$eq": self.value}}
        return {}


class RetrievalEndpoint(ModelEndpoint):
    def __init__(self, endpoint_name: str):
        self.endpoint_name = endpoint_name
        self.model_version = None
        self.model_display_name = None

    def init_input(self, user: User, params_in: PlaylistParamsIn):
        self.user = user
        self.user_input = str(self.user.id)
        self.params_in = params_in

    def get_params(self):
        params = []
        # dates filter
        if self.params_in.start_date is not None or self.params_in.end_date is not None:
            label = (
                "stock_beginning_date"
                if self.params_in.is_event
                else "offer_creation_date"
            )
            params.append(
                DateParams(
                    label=label,
                    min_val=self.params_in.start_date,
                    max_val=self.params_in.end_date,
                )
            )

        # stock_price
        if self.params_in.price_max is not None:
            price_max = min(
                self.user.user_deposit_remaining_credit, self.params_in.price_max
            )
        else:
            price_max = self.user.user_deposit_remaining_credit

        params.append(
            RangeParams(
                label="stock_price", min_val=self.params_in.price_min, max_val=price_max
            )
        )
        # search_group_names
        params.append(
            ListParams(
                label="search_group_names", values=self.params_in.search_group_names
            )
        )
        # subcategory_id
        params.append(
            ListParams(label="subcategory_id", values=self.params_in.subcategories_id)
        )
        params.append(EqParams(label="offer_is_duo", value=self.params_in.offer_is_duo))

        return {"$and": {k: v for d in params for k, v in d.filter().items()}}

    def model_score(self, item_input, size):
        start = time.time()
        instances = self.get_instance(size)
        prediction_result = endpoint_score(
            instances=instances, endpoint_name=self.endpoint_name
        )
        self.model_version = prediction_result.model_version
        self.model_display_name = prediction_result.model_display_name
        log_duration("retrieval_endpoint", start)
        # smallest = better
        results = {r["item_id"]: r["idx"] for r in results.predictions}

    def get_instance(self, size):
        return {
            "model_type": "filter",
            "order_by": "booking_number",
            "ascending": False,
            "size": size,
            "params": self.get_params(),
        }


class UserRetrievalEndpoint(RetrievalEndpoint):
    def get_instance(self, size):
        return {
            "model_type": "recommendation",
            "user_id": str(self.user.id),
            "size": size,
            "params": self.get_params(),
        }


class OfferRetrievalEndpoint(RetrievalEndpoint):
    def init_input(self, user: User, offer: Offer, params_in: PlaylistParamsIn):
        self.user = user
        self.offer = offer
        self.item_id = str(self.offer.id)
        self.params_in = params_in

    def model_score(self, item_input, size):
        start = time.time()
        instances = self.get_instance(size)
        prediction_result = endpoint_score(
            instances=instances, endpoint_name=self.endpoint_name
        )
        self.model_version = prediction_result.model_version
        self.model_display_name = prediction_result.model_display_name
        log_duration("retrieval_endpoint", start)
        # smallest = better
        results = {
            r["item_id"]: r["idx"]
            for r in results.predictions
            if r["item_id"] != self.item_id and " " not in r["item_id"]
        }

    def get_instance(self, size):
        return {
            "model_type": "similar_offer",
            "offer_id": str(self.item_id),
            "size": size,
            "params": self.get_params(),
        }
