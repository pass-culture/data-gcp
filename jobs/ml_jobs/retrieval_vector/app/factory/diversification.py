from typing import List

import numpy as np
from dppy.finite_dpps import FiniteDPP

DPP_SAMPLING_OUTPUT = 60


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
        self.K_DPP = DPP_SAMPLING_OUTPUT

    def get_sampled_ids(self):
        # Calculate weighted embeddings and normalize in a single step
        weighted_embeddings = self.scores[:, np.newaxis] * self.item_semantic_embeddings
        # Apply DPP sampling
        dpp = DPP(vectors=weighted_embeddings, K_DPP=self.K_DPP)
        sample_indices = dpp.sample_k()

        return [self.item_ids[i] for i in sample_indices[0]]
