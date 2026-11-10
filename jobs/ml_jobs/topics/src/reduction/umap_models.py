"""UMAP dimensionality reduction with a cuML (GPU) primary path and an
umap-learn (CPU) fallback (spec sections 7.2, 7.3's OOM-fallback rule
applied symmetrically here).
"""

from typing import Any

import numpy as np

from src.common.config import ReductionConfig
from src.common.gpu_fallback import fit_with_gpu_fallback


def umap_classes(device: str) -> type:
    """The UMAP class for ``device``: ``umap.UMAP`` on CPU, ``cuml.manifold.UMAP``
    on GPU (raising a clear ``ImportError`` if cuML isn't installed).
    """
    if device == "gpu":
        try:
            from cuml.manifold import UMAP
        except ImportError as error:
            raise ImportError(
                "device='gpu' needs RAPIDS cuML (install the `gpu` extra)"
            ) from error
        return UMAP
    if device == "cpu":
        from umap import UMAP

        return UMAP
    raise ValueError(f"device must be 'cpu' or 'gpu', got {device!r}")


def _free_gpu_memory() -> None:
    import cupy

    cupy.get_default_memory_pool().free_all_blocks()


def fit_umap(
    vectors: np.ndarray, config: ReductionConfig, device: str = "gpu"
) -> tuple[Any, np.ndarray]:
    """Fit UMAP on ``vectors`` (an ``(n_items, dim)`` float32 array) and
    return ``(model, reduced_vectors)``, trying ``device`` first and falling
    back to CPU on a GPU out-of-memory error.
    """

    def _fit(umap_device: str) -> tuple[Any, np.ndarray]:
        umap_cls = umap_classes(umap_device)
        kwargs: dict[str, object] = {
            "n_neighbors": config.n_neighbors,
            "n_components": config.n_components,
            "min_dist": config.min_dist,
            "metric": config.metric,
            "random_state": config.seed,
        }
        if umap_device == "cpu":
            # Pin umap-learn's current fit pipeline so a future minor
            # upgrade doesn't silently change embeddings.
            kwargs["compatibility_layout"] = True
        model = umap_cls(**kwargs)
        reduced = model.fit_transform(vectors)
        return model, reduced

    return fit_with_gpu_fallback(
        "reduction.fit_umap",
        gpu_fit=lambda: _fit("gpu"),
        cpu_fit=lambda: _fit("cpu"),
        device=device,
        free_gpu_memory=_free_gpu_memory,
    )
