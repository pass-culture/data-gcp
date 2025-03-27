from dppy.finite_dpps import FiniteDPP


class DPP:
    def __init__(self, vectors=None, K_DPP=150):
        self.vectors = vectors
        self.K_DPP = K_DPP
        self.DPP = FiniteDPP("likelihood", **{"L": self.vectors.T.dot(self.vectors)})

    def sample_k(self):
        self.DPP.sample_exact_k_dpp(size=self.K_DPP)
        return self.DPP.list_of_samples
