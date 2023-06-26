from pcreco.core.user import User
from pcreco.core.utils.vertex_ai import parallel_endpoint_score
from pcreco.utils.env_vars import log_duration
from typing import List, Dict, Any
import time
from pcreco.core.utils.cold_start import (
    get_cold_start_categories,
)
import random
import heapq


from pcreco.utils.env_vars import (
    MAX_RECO_ITEM_PER_BATCH,
    log_duration,
)
from pcreco.core.scorer import ModelEndpoint


class RecommendationEndpoint(ModelEndpoint):
    def __init__(self, endpoint_name: str):
        self.endpoint_name = endpoint_name
        self.model_version = None
        self.model_display_name = None

    def init_input(self, user: User):
        self.user = user
        self.user_input = str(self.user.id)

    def __get_instances(self, user_input, item_input) -> List[Dict[str, str]]:
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                chunk = lst[i : i + n]
                yield {"input_1": [user_input] * len(chunk), "input_2": chunk}

        return list(chunks(item_input, MAX_RECO_ITEM_PER_BATCH))

    def model_score(self, item_input, size: int = None):
        start = time.time()
        instances = self.__get_instances(self.user_input, item_input)
        log_duration(
            f"batch to score {len(instances)} for {self.user.id}",
            start,
        )
        predicted_scores = []
        start = time.time()
        for prediction_result in parallel_endpoint_score(self.endpoint_name, instances):
            self.model_version = prediction_result.model_version
            self.model_display_name = prediction_result.model_display_name
            predicted_scores.extend(prediction_result.predictions)

        log_duration(
            f"scored {len(predicted_scores)} batches for {self.user.id}",
            start,
        )
        start = time.time()

        recommendations = {
            item_id: predicted_scores[i][0] for i, item_id in enumerate(item_input)
        }
        if size is not None:
            recommendations = dict(
                heapq.nlargest(size, recommendations.items(), key=lambda item: item[1])
            )

        log_duration(
            f"heapq {len(predicted_scores)} batches for {self.user.id}",
            start,
        )

        return recommendations


class QPIEndpoint(RecommendationEndpoint):
    def init_input(self, user: User):
        self.user = user
        self.cold_start_categories = get_cold_start_categories(self.user.id)
        self.user_input = ",".join(self.cold_start_categories)


class DummyEndpoint(RecommendationEndpoint):
    def model_score(self, item_input, size: int = None):
        recommendations = {
            item_id: random.random() for i, item_id in enumerate(item_input)
        }
        if size is not None:
            recommendations = dict(
                heapq.nlargest(size, recommendations.items(), key=lambda item: item[1])
            )

        return recommendations
