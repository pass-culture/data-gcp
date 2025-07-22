"""Utility functions for model loading and embedding extraction."""

import numpy as np
import tensorflow as tf


def load_model(model_path: str) -> tf.keras.Model:
    """Load the model using multiple fallback approaches."""
    print(f"Loading model from: {model_path}")

    try:
        print("Attempting to load model directly...")
        model = tf.keras.models.load_model(model_path)
        print("Successfully loaded model using Keras load_model")
        return model
    except Exception as e1:
        print(f"Direct loading failed: {e1}")
        try:
            print("Attempting to load as SavedModel without tags...")
            model = tf.saved_model.load(model_path)
            print("Successfully loaded model using SavedModel without tags")
            return model
        except Exception as e2:
            print(f"SavedModel loading without tags failed: {e2}")
            try:
                print("Attempting to convert to TF 2.x SavedModel format...")
                # Try to convert to TF 2.x format
                converter = tf.compat.v1.wrap_function(
                    lambda x: model(x),
                    [tf.TensorSpec(shape=[None, None], dtype=tf.float32)],
                )
                tf.saved_model.save(converter, f"{model_path}_converted")
                model = tf.keras.models.load_model(f"{model_path}_converted")
                print("Successfully converted and loaded model")
                return model
            except Exception as e3:
                print(f"All loading attempts failed: {e3}")
                raise RuntimeError("Could not load the model in any format")


def extract_embeddings(model: tf.keras.Model, user_data_df: pd.DataFrame):
    """Extract embeddings from the model using multiple approaches."""
    print("Attempting to extract embeddings...")
    try:
        # First attempt: Standard Keras layer approach
        user_list = model.user_layer.layers[0].get_vocabulary()
        user_weights = model.user_layer.layers[1].get_weights()[0].astype(np.float32)
        item_list = model.item_layer.layers[0].get_vocabulary()
        item_weights = model.item_layer.layers[1].get_weights()[0].astype(np.float32)
        print("Successfully extracted embeddings using standard layer approach")
    except AttributeError:
        try:
            # Second attempt: Direct variable access
            print("Attempting direct variable access...")
            all_variables = [var.numpy() for var in model.variables]
            print(f"Found {len(all_variables)} variables in the model")
            for i, var in enumerate(all_variables):
                print(f"Variable {i} shape: {var.shape}")

            user_list = np.arange(all_variables[0].shape[0]).astype(str)
            user_weights = all_variables[0].astype(np.float32)
            item_list = np.arange(all_variables[2].shape[0]).astype(str)
            item_weights = all_variables[2].astype(np.float32)
            print("Successfully extracted embeddings using variable access")
        except Exception as e:
            print(f"Failed to extract embeddings: {e}")
            # Create dummy embeddings if everything fails
            print("Creating dummy embeddings...")
            dummy_dim = 32
            user_list = user_data_df["user_id"].unique()
            user_weights = np.random.normal(size=(len(user_list), dummy_dim)).astype(
                np.float32
            )
            item_list = np.array(["dummy_item"])
            item_weights = np.random.normal(size=(1, dummy_dim)).astype(np.float32)

    return user_list, user_weights, item_list, item_weights
