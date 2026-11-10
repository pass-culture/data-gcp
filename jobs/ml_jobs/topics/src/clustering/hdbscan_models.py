"""HDBSCAN clustering with a cuML (GPU) primary path and an hdbscan (CPU)
fallback (spec section 7.3).
"""

from typing import Any

import numpy as np

from src.common.config import ClusteringConfig
from src.common.gpu_fallback import fit_with_gpu_fallback

MIN_CLUSTER_SIZE_FLOOR = 50
MIN_CLUSTER_SIZE_CEILING = 150


def resolve_min_cluster_size(n_items: int, config: ClusteringConfig) -> int:
    """``clamp(n_items // 100, 50, 150)``, unless ``config.min_cluster_size``
    is set explicitly (not ``"auto"``).
    """
    if config.min_cluster_size != "auto":
        return config.min_cluster_size
    return max(MIN_CLUSTER_SIZE_FLOOR, min(n_items // 100, MIN_CLUSTER_SIZE_CEILING))


def hdbscan_classes(device: str) -> type:
    """The HDBSCAN class for ``device``: ``hdbscan.HDBSCAN`` on CPU,
    ``cuml.cluster.HDBSCAN`` on GPU (raising a clear ``ImportError`` if cuML
    isn't installed).
    """
    if device == "gpu":
        try:
            from cuml.cluster import HDBSCAN
        except ImportError as error:
            raise ImportError(
                "device='gpu' needs RAPIDS cuML (install the `gpu` extra)"
            ) from error
        return HDBSCAN
    if device == "cpu":
        from hdbscan import HDBSCAN

        return HDBSCAN
    raise ValueError(f"device must be 'cpu' or 'gpu', got {device!r}")


def _free_gpu_memory() -> None:
    import cupy

    cupy.get_default_memory_pool().free_all_blocks()


def fit_hdbscan(
    vectors: np.ndarray, config: ClusteringConfig, device: str = "gpu"
) -> Any:
    """Fit HDBSCAN on ``vectors`` (an ``(n_items, n_components)`` array),
    trying ``device`` first and falling back to CPU (with
    ``core_dist_n_jobs=config.cpu_fallback_jobs``, a CPU-only parallelism
    knob) on a GPU out-of-memory error. Returns the fitted model, with
    ``prediction_data=True`` so ``approximate_predict`` works on it later.
    """
    n_items = vectors.shape[0]
    min_cluster_size = resolve_min_cluster_size(n_items, config)
    max_cluster_size = int(config.max_cluster_share * n_items)

    def _fit(hdbscan_device: str) -> Any:
        hdbscan_cls = hdbscan_classes(hdbscan_device)
        kwargs: dict[str, object] = {
            "min_cluster_size": min_cluster_size,
            "min_samples": config.min_samples,
            "max_cluster_size": max_cluster_size,
            "metric": config.metric,
            "prediction_data": True,
        }
        if hdbscan_device == "cpu":
            kwargs["core_dist_n_jobs"] = config.cpu_fallback_jobs
        model = hdbscan_cls(**kwargs)
        model.fit(vectors)
        return model

    return fit_with_gpu_fallback(
        "clustering.fit_hdbscan",
        gpu_fit=lambda: _fit("gpu"),
        cpu_fit=lambda: _fit("cpu"),
        device=device,
        free_gpu_memory=_free_gpu_memory,
    )


def predict_clusters(
    model: Any, vectors: np.ndarray, device: str
) -> tuple[np.ndarray, np.ndarray]:
    """``(labels, strengths)`` for new points against a fitted model, using
    whichever library actually fit it (-1 in ``labels`` means outlier).
    """
    if device == "gpu":
        try:
            from cuml.cluster.hdbscan import approximate_predict
        except ImportError as error:
            raise ImportError(
                "device='gpu' needs RAPIDS cuML (install the `gpu` extra)"
            ) from error
    elif device == "cpu":
        from hdbscan.prediction import approximate_predict
    else:
        raise ValueError(f"device must be 'cpu' or 'gpu', got {device!r}")
    return approximate_predict(model, vectors)
