from datetime import datetime
import time
from dataclasses import dataclass
import typing as t
from abc import abstractmethod

from huggy.schemas.offer import Offer
from huggy.schemas.playlist_params import PlaylistParams
from huggy.schemas.user import User
from huggy.schemas.item import RecommendableItem

from huggy.core.endpoint import AbstractEndpoint

from huggy.utils.vertex_ai import endpoint_score
from huggy.utils.env_vars import log_duration


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
            return {
                self.label: {"$gte": float(self.min_val), "$lte": float(self.max_val)}
            }
        elif self.min_val is not None:
            return {self.label: {"$gte": float(self.min_val)}}
        elif self.max_val is not None:
            return {self.label: {"$lte": float(self.max_val)}}
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
            return {self.label: {"$eq": float(self.value)}}
        return {}


class RetrievalEndpoint(AbstractEndpoint):
    def init_input(self, user: User, params_in: PlaylistParams):
        self.user = user
        self.params_in = params_in
        self.user_input = str(self.user.user_id)
        self.is_geolocated = self.user.is_geolocated

    @abstractmethod
    def get_instance(self, size):
        pass

    def get_params(self):
        params = []

        if not self.is_geolocated:
            params.append(EqParams(label="is_geolocated", value=float(0.0)))

        if self.user.age and self.user.age < 18:
            params.append(EqParams(label="is_underage_recommendable", value=1))

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
                label="stock_price",
                min_val=self.params_in.price_min,
                max_val=price_max,
            )
        )
        # search_group_names
        params.append(
            ListParams(label="search_group_name", values=self.params_in.categories)
        )
        # subcategory_id
        params.append(
            ListParams(label="subcategory_id", values=self.params_in.subcategories)
        )
        params.append(EqParams(label="offer_is_duo", value=self.params_in.offer_is_duo))

        if self.params_in.offer_type_list is not None:
            label, domain = [], []
            for type in self.params_in.offer_type_list:
                domain.append(type.key)
                label.append(type.value)
            params.append(ListParams(label="offer_type_domain", values=domain))
            params.append(ListParams(label="offer_type_label", values=label))

        return {"$and": {k: v for d in params for k, v in d.filter().items()}}

    def model_score(self) -> t.List[RecommendableItem]:
        start = time.time()
        instances = self.get_instance(self.size)
        log_duration(f"retrieval_endpoint {instances}", start)
        prediction_result = endpoint_score(
            instances=instances,
            endpoint_name=self.endpoint_name,
            fallback_endpoints=self.fallback_endpoints,
        )
        self.model_version = prediction_result.model_version
        self.model_display_name = prediction_result.model_display_name
        log_duration("retrieval_endpoint", start)
        # smallest = better (cosine similarity or inner_product)
        return [
            RecommendableItem(
                item_id=r["item_id"],
                item_score=float(r.get("score", r["idx"])),
                item_rank=r["idx"],
            )
            for r in prediction_result.predictions
        ]


class FilterRetrievalEndpoint(RetrievalEndpoint):
    def get_instance(self, size: int):
        return {
            "model_type": "filter",
            "order_by": "booking_number",
            "ascending": False,
            "size": size,
            "params": self.get_params(),
        }


class RecommendationRetrievalEndpoint(RetrievalEndpoint):
    def get_instance(self, size: int):
        return {
            "model_type": "recommendation",
            "user_id": str(self.user.user_id),
            "size": size,
            "params": self.get_params(),
        }


class OfferRetrievalEndpoint(RetrievalEndpoint):
    def init_input(self, user: User, offer: Offer, params_in: PlaylistParams):
        self.user = user
        self.offer = offer
        self.item_id = str(self.offer.item_id)
        self.params_in = params_in
        self.is_geolocated = self.offer.is_geolocated

    def get_instance(self, size: int):
        return {
            "model_type": "similar_offer",
            "offer_id": str(self.item_id),
            "size": size,
            "params": self.get_params(),
        }


class OfferFilterRetrievalEndpoint(OfferRetrievalEndpoint):
    def get_instance(self, size: int):
        return {
            "model_type": "filter",
            "order_by": "booking_number",
            "ascending": False,
            "size": size,
            "params": self.get_params(),
        }
