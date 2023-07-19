from rii import Rii
from nanopq import PQ
import pandas as pd
import numpy as np
import pickle


class RiiModel:
    def set_up_index(self):
        size = self.model_weights.shape[0]
        train_vector = self.model_weights[
            np.random.choice(size, int(size * 0.3), replace=False)
        ]
        codec = PQ(M=32, Ks=256, verbose=True).fit(vecs=train_vector)
        self.index = Rii(fine_quantizer=codec)
        self.index.add_configure(vecs=self.model_weights, nlist=None, iter=5)

    def set_up_model(self):
        self.item_list = np.load("./metadata/items.npy", allow_pickle=True)
        self.model_weights = np.load(
            "./metadata/weights.npy", allow_pickle=True
        ).astype(np.float32)
        self.distance = len(self.model_weights[0])

        self.item_dict = {}
        for idx, (x, y) in enumerate(zip(self.item_list, self.model_weights)):
            self.item_dict[x] = {"embeddings": y, "idx": idx}

    def set_up_item_indexes(self):
        self.categories = {}
        df = pd.read_parquet("./metadata/item_metadata.parquet")
        for row in df.itertuples():
            idx = self.get_item_idx(row.item_id)
            if idx is not None:
                self.categories.setdefault(row.category, []).append(idx)

    def load(self, path):
        with open(path, "rb") as f:
            self.index = pickle.load(f)

    def save(self, path):
        with open(path, "wb") as f:
            pickle.dump(self.index, f)

    def get_idx_from_categories(self, categories):
        if categories is None:
            return None
        result = []
        for cat in categories:
            result.extend(self.categories.get(cat, []))
        return result

    def get_offer_emb(self, item_id):
        embs = self.item_dict.get(item_id, None)
        if embs is not None:
            return np.array(embs["embeddings"])
        else:
            return None

    def get_item_idx(self, item_id):
        embs = self.item_dict.get(item_id, None)
        if embs is not None:
            return embs["idx"]
        else:
            return None

    def selected_to_idx(self, selected_items):
        if selected_items is None:
            return None
        arr = []
        for item_id in selected_items:
            idx = self.get_item_idx(item_id)
            if idx is not None:
                arr.append(idx)
        return np.array(arr)

    def distances_to_offer(self, nn_idx, distances, n=10):
        if len(nn_idx) > n:
            nn_idx = nn_idx[:n]
            distances = distances[:n]
        return {
            "predictions": [
                self.item_list[index] for (index, _) in zip(nn_idx, distances)
            ]
        }

    def compute_distance(self, item_id, selected_idx=None, n=10):
        offer_emb = self.get_offer_emb(item_id)
        if offer_emb is not None:
            if selected_idx is not None:
                selected_idx = np.array(selected_idx)
            ids, dists = self.index.query(
                q=offer_emb, topk=n, target_ids=selected_idx, sort_target_ids=True
            )
            return self.distances_to_offer(nn_idx=ids, distances=dists, n=n)
        return {"predictions": []}
