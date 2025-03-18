import json

import numpy as np
from sklearn.gaussian_process.kernels import DotProduct


def sample_via_DPP(K, item_semantic_embeddings, scores):
    weighted_item_semantic_embeddings = item_semantic_embeddings * scores[:, None]
    dpp = DPP(
        range(len(weighted_item_semantic_embeddings)),
        weighted_item_semantic_embeddings,
    )
    dpp.preprocess()
    return dpp.sample_k(K)


def _convert_str_emb_to_float(emb_list, emb_size=64):
    float_emb = []
    for str_emb in emb_list:
        try:
            emb = json.loads(str_emb)
        except Exception:
            emb = [0] * emb_size
        float_emb.append(np.array(emb))
    return float_emb


def esym_poly(k, lam):
    N = lam.size
    E = np.zeros((k + 1, N + 1))
    E[0, :] = np.ones((1, N + 1))
    for ll in range(1, k + 1):
        for n in range(1, N + 1):
            E[ll, n] = E[ll, n - 1] + lam[n - 1] * E[ll - 1, n - 1]
    return E


def sample_k(k, lam, V_full):
    E = esym_poly(k, lam)
    J = []
    remaining = k - 1
    i = lam.size - 1
    while remaining >= 0:
        marg = 0.0
        if i == remaining:
            marg = 1.0
        else:
            if E[remaining + 1, i + 1] == 0:
                i -= 1
                continue
            marg = lam[i] * E[remaining, i] / E[remaining + 1, i + 1]
        if np.random.rand() < marg:
            J.append(i)
            remaining -= 1
        i -= 1
    k = len(J) - 1
    Y = np.zeros((len(J), 1))
    V = V_full[:, J]
    for i in range(k, -1, -1):
        Pr = np.sum(V**2, axis=1)
        Pr /= np.sum(Pr)
        C = np.cumsum(Pr)
        jj = np.argwhere(np.random.rand() <= C)[0]
        Y[i] = jj
        j = np.argwhere(V[int(Y[i]), :])[0]
        Vj = V[:, j]
        V = np.delete(V, j, 1)
        V -= np.outer(Vj, V[int(Y[i]), :] / Vj[int(Y[i])])
        if i > 0:
            V, r = np.linalg.qr(V)
    return Y


kernel = DotProduct(1.0, (1e-3, 1e3))


class DPP:
    def __init__(self, work_ids=None, vectors=None):
        self.work_ids = np.array(work_ids)
        self.vectors = vectors

    def compute_similarity(self):
        self.L = kernel(self.vectors)

    def preprocess(self, indices=None):
        self.compute_similarity()
        if indices is None:
            indices = list(range(len(self.vectors)))
        D, V = np.linalg.eig(self.L[np.ix_(indices, indices)])
        self.D = np.real(D)
        self.V = np.real(V)

    def preprocess_opti(self, indices=None):
        self.compute_similarity()
        if indices is None:
            indices = list(range(len(self.vectors)))
        D, V = np.linalg.eigh(self.L[np.ix_(indices, indices)])
        self.D = np.real(D)
        self.V = np.real(V)

    def sample_k(self, k):
        sampled_indices = [int(index) for index in sample_k(k, self.D, self.V)]
        return self.work_ids[sampled_indices]
