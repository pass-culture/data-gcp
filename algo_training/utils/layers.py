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
    """
    A preprocessing layer which maps string features into embeddings.

    When output_mode is "int", input strings are converted to their index in the vocabulary (an integer).
    When output_mode is "multi_hot", "count", or "tf_idf", input integers are encoded into an array where each dimension
    corresponds to an element in the vocabulary.
    """

    embedding_size: int

    def build_sequential_layer(self, vocabulary: np.ndarray):
        return tf.keras.Sequential(
            [
                StringLookup(vocabulary=vocabulary),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(vocabulary) + 1,
                    output_dim=self.embedding_size,
                ),
            ]
        )


@dataclass
class IntegerEmbeddingLayer:
    """
    A preprocessing layer which maps integer features into embeddings.

    When output_mode is "int", input integers are converted to their index in the vocabulary (an integer).
    When output_mode is "multi_hot", "count", or "tf_idf", input integers are encoded into an array where each dimension
    corresponds to an element in the vocabulary.
    """

    embedding_size: int

    def build_sequential_layer(self, vocabulary: np.ndarray):
        return tf.keras.Sequential(
            [
                String2IntegerLayer(),
                IntegerLookup(vocabulary=vocabulary.astype(int)),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(vocabulary) + 1,
                    output_dim=self.embedding_size,
                ),
            ]
        )


@dataclass
class TextEmbeddingLayer:
    """
    Preprocessing layer which maps text features to integer sequences.

    This layer has basic options for managing text in a Keras model.
    The processing of each example contains the following steps:
        - Standardize each example (usually lowercasing + punctuation stripping)
        - Split each example into substrings (usually words)
        - Recombine substrings into tokens (usually ngrams)
        - Index tokens (associate a unique int value with each token)
        - Transform each example using this index, either into a vector of ints or a dense float vector.

    The vocabulary for the layer must be either supplied on construction or learned via adapt().

    When output_mode is "int", input integers are converted to their index in the vocabulary (an integer).
    When output_mode is "multi_hot", "count", or "tf_idf", input integers are encoded into an array where each dimension
    corresponds to an element in the vocabulary.
    """

    embedding_size: int
    max_tokens: int = 10000

    def build_sequential_layer(self, vocabulary: np.ndarray):
        text_dataset = tf.data.Dataset.from_tensor_slices(vocabulary)
        text_vectorization_layer = TextVectorization(
            max_tokens=self.max_tokens,
            output_mode="multi_hot",
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
    """
    Preprocessing layer which casts string representations of numbers into integers
    """

    def __init__(self, output_type: tf.DType = tf.int32):
        super().__init__()

        self.output_type = output_type

    def call(self, inputs):
        return tf.strings.to_number(inputs, self.output_type)
