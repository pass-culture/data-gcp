import numpy as np
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Embedding, Flatten, Input, Dense
from tensorflow.keras.layers import Lambda, Dot
from tensorflow.keras.regularizers import l2


class MatchModel(Model):
    def __init__(self, user_layer, item_layer):
        super().__init__(name="MatchModel")

        self.user_layer = user_layer
        self.item_layer = item_layer

        self.flatten = Flatten()
        self.dot = Dot(axes=1, normalize=True)

    def call(self, inputs):
        user_input = inputs[0]
        pos_item_input = inputs[1]

        user_embedding = self.user_layer(user_input)
        user_embedding = self.flatten(user_embedding)

        pos_item_embedding = self.item_layer(pos_item_input)
        pos_item_embedding = self.flatten(pos_item_embedding)

        pos_similarity = self.dot([user_embedding, pos_item_embedding])

        return pos_similarity
