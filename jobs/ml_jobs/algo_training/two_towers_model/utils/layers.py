import json
from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np
import tensorflow as tf
from loguru import logger
from tensorflow.keras.layers import (
    Embedding,
    StringLookup,
    TextVectorization,
)


class CustomInputLayer(ABC):
    @abstractmethod
    def build_sequential_layer(self, input_data: np.ndarray) -> tf.keras.Sequential:
        return NotImplementedError


@dataclass
class StringEmbeddingLayer(CustomInputLayer):
    """StringEmbeddingLayer for creating string embedding layers.
    This layer converts string inputs to dense embeddings by first performing
    a vocabulary lookup and then mapping to a fixed-size embedding.
    Attributes:
        embedding_size: The dimensionality of the embedding output.
    Methods:
        build_sequential_layer: Builds a sequential model for string embedding.
            Args:
                input_data: A numpy array containing the vocabulary strings.
            Returns:
                A tf.keras.Sequential model that performs string lookup and embedding.
    """

    embedding_size: int

    def build_sequential_layer(self, input_data: np.ndarray) -> tf.keras.Sequential:
        """
        Creates a sequential layer with string lookup and embedding.
        This method builds a sequential model that first converts string inputs to indices
        using a vocabulary derived from the input data, then maps those indices to
        dense embeddings of the specified size.
        Args:
            input_data (np.ndarray): An array containing the vocabulary strings to be used
                                     for the StringLookup layer.
        Returns:
            tf.keras.Sequential: A sequential model with StringLookup and Embedding layers.
        """

        return tf.keras.Sequential(
            [
                StringLookup(vocabulary=input_data),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(input_data) + 1,
                    output_dim=self.embedding_size,
                ),
            ]
        )


@dataclass
class NumericalFeatureProcessor(CustomInputLayer):
    """A custom layer for processing numerical features. This layer normalizes the input data
    and then applies a Dense layer with a specified number of units.
    Attributes:
        output_size (int): Number of units in the Dense layer.
    Methods:
        build_sequential_layer(input_data: np.ndarray) -> tf.keras.Sequential:
            Builds a sequential layer that normalizes the input data and applies a Dense layer.
        Args:
            input_data (np.ndarray): The input data to be processed.
        Returns:
            tf.keras.Sequential: A sequential model with normalization and Dense layers.
    """

    output_size: int  # Number of units in the Dense layer

    def build_sequential_layer(self, input_data: np.ndarray) -> tf.keras.Sequential:
        """
        Builds a sequential layer with normalization and dense layers.
        This method creates a sequential model that applies normalization using
        precomputed mean and variance from the input data, followed by a dense layer.
        Args:
            input_data: A numpy array containing the data to be used for computing
                normalization statistics.
        Returns:
            A tf.keras.Sequential model with a normalization layer and a dense layer.
        Note:
            The output size of the dense layer is determined by the class attribute
            `output_size`.
        """

        # Precompute mean/variance offline
        mean = np.mean(input_data.astype("float32"))
        variance = np.var(input_data.astype("float32"))

        # Create and adapt the normalization layer to the data
        normalization = tf.keras.layers.Normalization(
            input_shape=(1,), mean=mean, variance=variance
        )

        return tf.keras.Sequential(
            [normalization, tf.keras.layers.Dense(self.output_size)]
        )


@dataclass
class TextEmbeddingLayer(CustomInputLayer):
    """TextEmbeddingLayer creates a text embedding sequential model.
    This layer converts text into fixed-length embeddings by first vectorizing the input text,
    then applying an embedding layer, and finally using global average pooling to create
    a fixed-size representation.
    Attributes:
        embedding_size: The dimension of the embedding vectors.
        max_tokens: The maximum number of tokens to keep in the vocabulary.
    Returns:
        A Sequential Keras model that converts text input to embeddings.
    """

    embedding_size: int
    max_tokens: int = 10000

    def build_sequential_layer(self, input_data: np.ndarray) -> tf.keras.Sequential:
        """
        Builds a sequential layer for text processing.
        This method creates a TensorFlow Sequential model that processes text data through
        vectorization, embedding, and pooling layers.
        Args:
            input_data (np.ndarray): The input text data to be processed and used for adapting
                                    the text vectorization layer.
        Returns:
            tf.keras.Sequential: A sequential model consisting of:
                - A TextVectorization layer with max_tokens set to self.max_tokens
                - An Embedding layer with output dimension of self.embedding_size
                - A GlobalAveragePooling1D layer
        """

        text_dataset = tf.data.Dataset.from_tensor_slices(input_data)
        text_vectorization_layer = TextVectorization(
            max_tokens=self.max_tokens,
            output_mode="int",
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


@dataclass
class PretainedEmbeddingLayer(CustomInputLayer):
    """PretainedEmbeddingLayer creates a non-trainable embedding layer initialized with pre-trained weights.
    This class inherits from CustomInputLayer and builds a sequential layer that consists of a StringLookup
    followed by an Embedding layer initialized with pre-trained weights.
    Attributes:
        embedding_size (int): The dimensionality of the embedding vectors.
        embedding_initialisation_weights (list): A list of string representations of embedding vectors to
            initialize the embedding matrix.
    Methods:
        convert_str_emb_to_matrix(emb_list, emb_size): Converts a list of string embeddings to a numeric matrix.
        build_sequential_layer(vocabulary): Builds and returns a sequential layer with StringLookup and Embedding.
    """

    embedding_size: int
    embedding_initialisation_weights: list

    def convert_str_emb_to_matrix(self, emb_list, emb_size):
        """
        Convert a list of string embeddings to a matrix.
        This function takes a list of string embeddings, where each string is expected
        to be a JSON representation of a list of floats. It then converts each string
        into an array and stacks them to form a matrix. The first row of the matrix
        is reserved for an unknown token, represented as a zero vector.
        Args:
            emb_list (list): List of string embeddings, each is a JSON string representing a list of floats.
            emb_size (int): Size of each embedding vector.
        Returns:
            numpy.matrix: A matrix containing the embeddings, with the first row
                being a zero vector for the unknown token.
        Note:
            If parsing a string embedding fails, it will be replaced with a zero vector.
        """

        # here we account for UNK token
        float_emb = [np.array([0] * emb_size)]
        for str_emb in emb_list:
            try:
                emb = json.loads(str_emb)
            except Exception:
                emb = [0] * emb_size
            float_emb.append(np.array(emb))
        emb_matrix = np.matrix(float_emb)
        return emb_matrix

    def build_sequential_layer(self, input_data: np.ndarray):
        """
        Builds a sequential layer with pretrained embeddings for text processing.
        This method creates a sequential layer that first performs string lookup using the provided vocabulary
        and then applies pretrained embeddings. The embedding weights are initialized from the class's
        embedding_initialisation_weights attribute and are set to be non-trainable.
        Args:
            input_data (np.ndarray): Array of vocabulary terms for the StringLookup layer.
        Returns:
            tf.keras.Sequential: A sequential model with StringLookup and Embedding layers.
        Note:
            The embedding size is determined by the self.embedding_size attribute.
            The embedding weights are loaded from self.embedding_initialisation_weights.
        """

        logger.info("Pretained layer...")
        logger.info("Init emb...")
        pretrained_emb = self.convert_str_emb_to_matrix(
            self.embedding_initialisation_weights, self.embedding_size
        )
        embedding = Embedding(
            input_dim=len(pretrained_emb),
            output_dim=self.embedding_size,
            weights=[pretrained_emb],
            trainable=False,
            name="embedding",
        )
        logger.info("Building sequential...")
        pretained_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=input_data),
                embedding,
            ]
        )
        logger.info("Return pretrained_layer")
        return pretained_layer
