import numpy as np
import faiss


def compute_distance_subset(index, xq, subset):
    n, _ = xq.shape
    _, k = subset.shape
    distances = np.empty((n, k), dtype=np.float32)
    index.compute_distance_subset(
        n, faiss.swig_ptr(xq), k, faiss.swig_ptr(distances), faiss.swig_ptr(subset)
    )
    return distances


class FaissModel:
    def set_up_index(self):
        self.index = faiss.IndexFlatL2(self.distance)
        self.index.add(self.model_weights)

    def set_up_model(self):
        import tensorflow as tf

        tf_reco = tf.keras.models.load_model("./model/")
        self.item_list = tf_reco.item_layer.layers[0].get_vocabulary()
        self.model_weights = tf_reco.item_layer.layers[1].get_weights()[0]
        self.distance = len(self.model_weights[0])

        self.item_dict = {}
        for idx, (x, y) in enumerate(zip(self.item_list, self.model_weights)):
            self.item_dict[x] = {"embeddings": y, "idx": idx}

    def set_up_item_indexes(self):
        import pandas as pd

        self.categories = {}
        df = pd.read_parquet("./metadata/item_metadata.parquet")
        for _, row in df.iterrows():
            idx = self.get_item_idx(row["item_id"])
            if idx is not None:
                self.categories.setdefault(row["category"], []).append(idx)

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
            return np.array([embs["embeddings"]])
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

    def distances_to_offer(self, nn_idx, n=10):
        if len(nn_idx) > n:
            nn_idx = nn_idx[:n]
        return {"predictions": [self.item_list[index] for (index, _) in nn_idx]}

    def compute_distance(self, item_id, selected_idx=None, n=10):
        offer_emb = self.get_offer_emb(item_id)
        if offer_emb is not None:
            if selected_idx is not None:
                distances = compute_distance_subset(
                    self.index, offer_emb, np.array([selected_idx])
                ).tolist()[0]
                nn_idx = sorted(
                    zip(list(selected_idx), list(distances)), key=lambda tup: tup[1]
                )
            else:
                results = self.index.search(
                    offer_emb,
                    k=n,
                )
                # indexes, distances
                nn_idx = sorted(
                    zip(list(results[1][0]), list(results[0][0])),
                    key=lambda tup: tup[1],
                )

            return self.distances_to_offer(nn_idx=nn_idx, n=n)
        return {"predictions": []}
