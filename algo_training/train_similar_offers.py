from annoy import AnnoyIndex
import tensorflow as tf
from utils import remove_dir, SIMILAR_OFFERS_DIR

model_A = tf.keras.models.load_model(f"{SIMILAR_OFFERS_DIR}/model/tf_reco")
embedding_item_model = model_A.item_layer.layers[1].get_weights()
item_list_model = model_A.item_layer.layers[0].get_vocabulary()

a = AnnoyIndex(64, "euclidean")
for index, vector in enumerate(embedding_item_model[0].tolist()):
    a.add_item(index, vector)
a.build(-1)
a.save(f"{SIMILAR_OFFERS_DIR}/model/sim_offers.ann")
