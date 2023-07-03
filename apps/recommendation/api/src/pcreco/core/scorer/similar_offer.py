from pcreco.core.offer import Offer
from pcreco.core.utils.vertex_ai import endpoint_score
from pcreco.utils.env_vars import log_duration

import time
import random
import heapq
from pcreco.core.scorer import ModelEndpoint
from pcreco.utils.env_vars import (
    log_duration,
)


class SimilarOfferEndpoint(ModelEndpoint):
    def __init__(self, endpoint_name: str):
        self.endpoint_name = endpoint_name
        self.model_version = None
        self.model_display_name = None

    def init_input(self, offer: Offer):
        self.offer = offer

    def model_score(self, item_input, size):
        start = time.time()
        instances = {
            "offer_id": self.offer.item_id,
            "selected_offers": item_input,
            "size": size,
        }
        prediction_result = endpoint_score(
            instances=instances, endpoint_name=self.endpoint_name
        )
        self.model_version = prediction_result.model_version
        self.model_display_name = prediction_result.model_display_name
        log_duration("similar_offer_model_score", start)
        return {
            item_id: size - i
            for i, item_id in enumerate(prediction_result.predictions)
            if item_id != self.offer.item_id and " " not in item_id
        }


class SimilarOfferV2Endpoint(ModelEndpoint):
    def __init__(self, endpoint_name: str):
        self.endpoint_name = endpoint_name
        self.model_version = None
        self.model_display_name = None

    def init_input(self, offer: Offer):
        self.offer = offer

    def model_score(self, item_input, size):
        start = time.time()
        if item_input is not None and len(item_input) > 0:
            instances = {
                "offer_id": self.offer.item_id,
                "selected_categories": item_input,
                "size": size,
            }
        else:
            instances = {
                "offer_id": self.offer.item_id,
                "size": size,
            }
        prediction_result = endpoint_score(
            instances=instances, endpoint_name=self.endpoint_name
        )
        self.model_version = prediction_result.model_version
        self.model_display_name = prediction_result.model_display_name
        log_duration("similar_offer_model_score", start)
        return {
            item_id: size - i
            for i, item_id in enumerate(prediction_result.predictions)
            if item_id != self.offer.item_id and " " not in item_id
        }


class DummyEndpoint(SimilarOfferEndpoint):
    def model_score(self, item_input, size: int = None):
        recommendations = {
            item_id: random.random() for i, item_id in enumerate(item_input)
        }
        if size is not None:
            recommendations = dict(
                heapq.nlargest(size, recommendations.items(), key=lambda item: item[1])
            )

        return recommendations
