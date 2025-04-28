from typing import List

import numpy as np
from dppy.finite_dpps import FiniteDPP


class DiversificationPipeline:
    def __init__(
        self, item_semantic_embeddings: List, ids: List, scores: List, use_qi: bool
    ) -> None:
        self.item_semantic_embeddings = np.array(
            item_semantic_embeddings, dtype=np.float64
        )
        self.item_ids = ids
        self.scores = np.array(scores, dtype=np.float64)
        self.use_qi = use_qi

    @staticmethod
    def _get_dpp_samples(vectors: np.ndarray, K_DPP: int) -> List:
        dpp_model = FiniteDPP("likelihood", **{"L": vectors.dot(vectors.T)})
        dpp_result = dpp_model.sample_exact_k_dpp(size=K_DPP)
        return dpp_result.list_of_samples[0]

    def get_sampled_ids(self, K_DPP: int, use_qi: bool) -> List:
        # Calculate weighted embeddings and normalize in a single step
        weighted_embeddings = (
            self.scores[:, np.newaxis] * self.item_semantic_embeddings
            if use_qi
            else self.item_semantic_embeddings
        )

        # Apply DPP sampling
        sampled_indices = self._get_dpp_samples(
            vectors=weighted_embeddings, K_DPP=K_DPP
        )

        return [self.item_ids[i] for i in sampled_indices]
