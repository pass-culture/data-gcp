import time

import numpy as np
import tensorflow as tf
import umap
from loguru import logger
from sklearn.decomposition import PCA


def get_sample(data, frac):
    sample_size = int(frac * len(data))
    return data[np.random.choice(len(data), size=sample_size, replace=False)]


def umap_reduce_embedding_dimension(
    data,
    dimension,
    return_model=False,
    use_gpu=False,
    n_neighbors=10,
    metric="cosine",
):
    """
    Reduce dimensionality using UMAP with optional GPU acceleration.

    Args:
        data: Input data array
        dimension: Target dimension
        return_model: Whether to return the fitted model along with transformed data
        use_gpu: Whether to use GPU acceleration if available
        n_neighbors: Number of neighbors for UMAP
        metric: Distance metric to use

    Returns:
        If return_model is False: transformed data array
        If return_model is True: tuple of (transformed data, fitted model)
    """
    start_time = time.time()
    logger.info(f"Starting UMAP reduction to {dimension} dimensions")

    try:
        if use_gpu:
            # Try using RAPIDS cuML UMAP if available (GPU accelerated)
            try:
                import cuml

                logger.info("Using RAPIDS cuML GPU-accelerated UMAP")

                umap_model = cuml.UMAP(
                    n_neighbors=n_neighbors,
                    n_components=dimension,
                    init="random",
                    metric=metric,
                )
                transformed = umap_model.fit_transform(data)

            except ImportError:
                # Fall back to standard UMAP with numba GPU target
                logger.info(
                    "RAPIDS cuML not available, using standard UMAP with GPU target"
                )
                umap_model = umap.UMAP(
                    n_neighbors=n_neighbors,
                    n_components=dimension,
                    init="random",
                    metric=metric,
                    low_memory=False,  # Better for GPU
                    unique=True,
                    n_jobs=-1,  # Use all available cores
                )
                transformed = umap_model.fit_transform(data)
        else:
            # Standard CPU implementation
            logger.info("Using standard UMAP with CPU")
            umap_model = umap.UMAP(
                n_neighbors=n_neighbors,
                n_components=dimension,
                init="random",
                metric=metric,
                low_memory=True,
                unique=True,
                n_jobs=-1,  # Use all available cores
            )
            transformed = umap_model.fit_transform(data)

        logger.info(
            f"UMAP reduction completed in {time.time() - start_time:.2f} seconds"
        )

        if return_model:
            return transformed, umap_model
        else:
            return transformed

    except Exception as e:
        logger.error(f"Error during UMAP reduction: {e}")
        raise


def pumap_reduce_embedding_dimension(
    data, dimension, train_frac=0.1, batch_size=2048, return_model=False
):
    """
    Parametric UMAP implementation with performance optimizations.

    Args:
        data: Input data array
        dimension: Target dimension
        train_frac: Fraction of data to use for training
        batch_size: Batch size for training
        return_model: Whether to return the model along with transformed data

    Returns:
        If return_model is False: transformed data
        If return_model is True: tuple of (transformed data, fitted model)
    """
    start_time = time.time()
    logger.info(f"Starting Parametric UMAP reduction to {dimension} dimensions")

    keras_fit_kwargs = {
        "callbacks": [
            tf.keras.callbacks.EarlyStopping(
                monitor="loss",
                min_delta=10**-2,
                patience=2,
                verbose=1,
            )
        ],
        "verbose": 2,
    }

    # Try to use mixed precision if available
    try:
        policy = tf.keras.mixed_precision.Policy("mixed_float16")
        tf.keras.mixed_precision.set_global_policy(policy)
        logger.info("Using mixed precision for Parametric UMAP")
    except Exception:
        logger.info("Mixed precision not available, using default precision")

    embedder = umap.ParametricUMAP(
        n_components=dimension,
        n_neighbors=10,
        verbose=True,
        metric="cosine",
        keras_fit_kwargs=keras_fit_kwargs,
        n_training_epochs=10,
        batch_size=batch_size,
    )

    sample_data = get_sample(data, train_frac)
    logger.info(f"Fitting Parametric UMAP on {len(sample_data)} samples")

    fitted_model = embedder.fit(sample_data)
    logger.info(f"Transforming {len(data)} samples with fitted PUMAP model")

    transformed = fitted_model.transform(data)
    logger.info(f"PUMAP reduction completed in {time.time() - start_time:.2f} seconds")

    if return_model:
        return transformed, fitted_model
    else:
        return transformed


def pca_reduce_embedding_dimension(data, dimension, seed=0):
    """
    PCA dimension reduction with performance tracking.

    Args:
        data: Input data array
        dimension: Target dimension
        seed: Random seed for reproducibility

    Returns:
        Transformed data array
    """
    start_time = time.time()
    logger.info(f"Starting PCA reduction to {dimension} dimensions")

    # Use randomized PCA for very large datasets
    if data.shape[1] > 100 and data.shape[0] > 10000:
        logger.info("Using randomized PCA for large dataset")
        pca = PCA(n_components=dimension, random_state=seed, svd_solver="randomized")
    else:
        pca = PCA(n_components=dimension, random_state=seed)

    transformed = pca.fit_transform(data)
    logger.info(f"PCA reduction completed in {time.time() - start_time:.2f} seconds")
    return transformed
