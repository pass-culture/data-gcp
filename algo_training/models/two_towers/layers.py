from dataclasses import dataclass

import numpy as np
import tensorflow as tf

from tensorflow.keras.layers import Embedding, TextVectorization, Dot
from tensorflow.keras.layers.experimental.preprocessing import (
    StringLookup,
    IntegerLookup,
)


@dataclass
class StringEmbeddingLayer:
    embedding_size: int

    def build_sequential_layer(self, vocabulary: np.ndarray):
        return tf.keras.Sequential(
            [
                StringLookup(vocabulary=vocabulary),
                Embedding(
                    input_dim=len(vocabulary) + 1,
                    output_dim=self.embedding_size,
                ),
            ]
        )


@dataclass
class IntegerEmbeddingLayer:
    embedding_size: int

    def build_sequential_layer(self, vocabulary: np.ndarray):
        return tf.keras.Sequential(
            [
                String2IntegerLayer(),
                IntegerLookup(vocabulary=vocabulary.astype(int)),
                Embedding(
                    input_dim=len(vocabulary) + 1,
                    output_dim=self.embedding_size,
                ),
            ]
        )


@dataclass
class TextEmbeddingLayer:
    embedding_size: int
    max_tokens: int = 10000

    def build_sequential_layer(self, vocabulary: np.ndarray):
        text_dataset = tf.data.Dataset.from_tensor_slices(vocabulary)
        text_vectorization_layer = TextVectorization(
            max_tokens=self.max_tokens,
            output_mode="tf_idf",
        )
        text_vectorization_layer.adapt(text_dataset.batch(64))
        return tf.keras.Sequential(
            [
                text_vectorization_layer,
                Embedding(
                    input_dim=self.max_tokens,
                    output_dim=self.embedding_size,
                ),
                tf.keras.layers.GlobalAveragePooling1D(),
            ]
        )


class String2IntegerLayer(tf.keras.layers.Layer):
    def __init__(self, output_type: tf.DType = tf.int32):
        super().__init__()

        self.output_type = output_type

    def call(self, inputs):
        return tf.strings.to_number(inputs, self.output_type)
