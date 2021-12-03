from tensorflow.keras.models import Model
from tensorflow.keras.layers import Embedding, Flatten, Input, Dense, Lambda, Dot


class DeepMatchModel(Model):
    def __init__(self, user_layer, item_layer, subcategory_layer, mlp):
        super(DeepMatchModel, self).__init__()

        self.user_layer = user_layer
        self.item_layer = item_layer
        self.subcategory_layer = subcategory_layer
        self.mlp = mlp

        self.flatten = Flatten()
        self.concat = Concatenate()

    def call(self, inputs):
        user_input = inputs[0]
        pos_item_input = inputs[1]
        pos_item_subcategory_input = inputs[2]

        user_embedding = self.flatten(self.user_layer(user_input))
        pos_item_embedding = self.flatten(self.item_layer(pos_item_input))
        pos_item_subcategory = self.flatten(
            self.subcategory_layer(pos_item_subcategory_input)
        )

        pos_tensor = self.concat(
            [user_embedding, pos_item_embedding, pos_item_subcategory]
        )

        pos_similarity = self.mlp(pos_tensor)

        return pos_similarity

    def get_config(self):
        return {
            "user_layer": self.user_layer,
            "item_layer": self.item_layer,
            "subcategory_layer": self.subcategory_layer,
            "mlp": self.mlp,
        }

    @classmethod
    def from_config(cls, config):
        return cls(**config)
