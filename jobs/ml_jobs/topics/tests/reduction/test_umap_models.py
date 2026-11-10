import numpy as np
import pytest
import umap

from src.common.config import ReductionConfig, load_config
from src.reduction import umap_models as umap_models_module
from src.reduction.umap_models import fit_umap, umap_classes


def test_umap_classes_cpu_returns_umap_learn() -> None:
    assert umap_classes("cpu") is umap.UMAP


def test_umap_classes_invalid_device_raises() -> None:
    with pytest.raises(ValueError, match="device must be"):
        umap_classes("tpu")


def test_umap_classes_gpu_raises_clear_error_without_cuml() -> None:
    try:
        import cuml  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("cuML is installed; this tests the missing-cuML path")

    with pytest.raises(ImportError, match="RAPIDS cuML"):
        umap_classes("gpu")


def _reduction_config() -> ReductionConfig:
    return load_config("default").reduction


def test_fit_umap_cpu_returns_model_and_reduced_vectors() -> None:
    rng = np.random.default_rng(0)
    vectors = rng.normal(size=(60, 16)).astype(np.float32)
    config = _reduction_config()

    model, reduced = fit_umap(vectors, config, device="cpu")

    assert reduced.shape == (60, config.n_components)
    assert reduced.dtype == np.float32
    assert hasattr(model, "transform")


def test_fit_umap_falls_back_to_cpu_on_gpu_oom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExplodingUMAP:
        def __init__(self, **kwargs: object) -> None:
            pass

        def fit_transform(self, vectors: np.ndarray) -> np.ndarray:
            raise MemoryError("simulated GPU OOM")

    def fake_umap_classes(device: str) -> type:
        return ExplodingUMAP if device == "gpu" else umap.UMAP

    monkeypatch.setattr(umap_models_module, "umap_classes", fake_umap_classes)
    monkeypatch.setattr(umap_models_module, "_free_gpu_memory", lambda: None)

    rng = np.random.default_rng(0)
    vectors = rng.normal(size=(60, 16)).astype(np.float32)
    config = _reduction_config()

    model, reduced = fit_umap(vectors, config, device="gpu")

    assert reduced.shape == (60, config.n_components)
    assert isinstance(model, umap.UMAP)
