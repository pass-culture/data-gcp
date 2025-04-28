from typing import List

import numpy as np
from dppy.finite_dpps import FiniteDPP


class DPP:
    def __init__(self, vectors=None, K_DPP=150):
        self.vectors = vectors.T
        self.K_DPP = K_DPP
        self.DPP = FiniteDPP("likelihood", **{"L": self.vectors.T.dot(self.vectors)})

    def sample_k(self):
        self.DPP.sample_exact_k_dpp(size=self.K_DPP)
        return self.DPP.list_of_samples


class DiversificationPipeline:
    def __init__(self, item_semantic_embeddings: List, ids: List, scores: List) -> None:
        self.item_semantic_embeddings = np.array(
            item_semantic_embeddings, dtype=np.float64
        )
        self.item_ids = ids
        self.scores = np.array(scores, dtype=np.float64)
        self.K_DPP = 60

    def get_sampled_ids(self):
        # Normalize scores (min-max scaling)
        min_score = np.min(self.scores)
        max_score = np.max(self.scores)

        # Avoid division by zero
        if max_score - min_score < 1e-10:
            normalized_scores = np.ones_like(self.scores)
        else:
            normalized_scores = (self.scores - min_score) / (max_score - min_score)

        # Calculate weighted embeddings and normalize in a single step
        weighted_embeddings = (
            normalized_scores[:, np.newaxis] * self.item_semantic_embeddings
        )

        # L2 normalization
        norms = np.linalg.norm(weighted_embeddings, axis=1, keepdims=True)
        norms[norms < 1e-10] = 1.0
        normalized_weighted_embeddings = weighted_embeddings / norms

        # Apply DPP sampling
        dpp = DPP(vectors=normalized_weighted_embeddings, K_DPP=self.K_DPP)
        sample_indices = dpp.sample_k()

        # Return sampled IDs
        indices = (
            sample_indices[0] if isinstance(sample_indices, list) else sample_indices
        )
        return [self.item_ids[i] for i in indices]
