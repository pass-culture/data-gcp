import hdbscan as hdbscan_cpu_module
import numpy as np
import pytest

from src.clustering import hdbscan_models as hdbscan_models_module
from src.clustering.hdbscan_models import (
    fit_hdbscan,
    hdbscan_classes,
    resolve_min_cluster_size,
)
from src.common.config import ClusteringConfig, load_config


def _clustering_config(**overrides: object) -> ClusteringConfig:
    config = load_config("default").clustering
    return config.model_copy(update=overrides)


def test_resolve_min_cluster_size_auto_clamps_floor() -> None:
    config = _clustering_config(min_cluster_size="auto")
    assert resolve_min_cluster_size(1000, config) == 50


def test_resolve_min_cluster_size_auto_clamps_ceiling() -> None:
    config = _clustering_config(min_cluster_size="auto")
    assert resolve_min_cluster_size(20000, config) == 150


def test_resolve_min_cluster_size_auto_uses_formula_in_range() -> None:
    config = _clustering_config(min_cluster_size="auto")
    assert resolve_min_cluster_size(10000, config) == 100


def test_resolve_min_cluster_size_explicit_value_used_as_is() -> None:
    config = _clustering_config(min_cluster_size=42)
    assert resolve_min_cluster_size(10000, config) == 42


def test_hdbscan_classes_cpu_returns_hdbscan() -> None:
    assert hdbscan_classes("cpu") is hdbscan_cpu_module.HDBSCAN


def test_hdbscan_classes_invalid_device_raises() -> None:
    with pytest.raises(ValueError, match="device must be"):
        hdbscan_classes("tpu")


def test_hdbscan_classes_gpu_raises_clear_error_without_cuml() -> None:
    try:
        import cuml  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("cuML is installed; this tests the missing-cuML path")

    with pytest.raises(ImportError, match="RAPIDS cuML"):
        hdbscan_classes("gpu")


def _synthetic_blobs() -> np.ndarray:
    """Two 100-point blobs plus 800 scattered background points, so each
    blob is a 10% minority of the corpus -- comfortably under the default
    ``max_cluster_share`` (20%) cap, and realistic (spec section 3's corpora
    are 25-83% outliers).
    """
    rng = np.random.default_rng(0)
    blob1 = rng.normal(loc=0, scale=0.3, size=(100, 5))
    blob2 = rng.normal(loc=8, scale=0.3, size=(100, 5))
    background = rng.uniform(low=-10, high=18, size=(800, 5))
    return np.vstack([blob1, blob2, background]).astype(np.float32)


def test_fit_hdbscan_cpu_separates_two_blobs() -> None:
    vectors = _synthetic_blobs()
    config = _clustering_config(min_cluster_size=50)

    model = fit_hdbscan(vectors, config, device="cpu")
    labels = model.labels_

    blob1_labels = set(labels[:100]) - {-1}
    blob2_labels = set(labels[100:200]) - {-1}
    assert blob1_labels
    assert blob2_labels
    assert blob1_labels.isdisjoint(blob2_labels)


def test_fit_hdbscan_passes_max_cluster_size_and_cpu_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class RecordingHDBSCAN:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        def fit(self, vectors: np.ndarray) -> None:
            self.labels_ = np.zeros(len(vectors))
            self.probabilities_ = np.ones(len(vectors))

    monkeypatch.setattr(
        hdbscan_models_module, "hdbscan_classes", lambda device: RecordingHDBSCAN
    )

    vectors = _synthetic_blobs()
    config = _clustering_config(min_cluster_size=20, max_cluster_share=0.2)
    fit_hdbscan(vectors, config, device="cpu")

    assert captured["max_cluster_size"] == int(0.2 * len(vectors))
    assert captured["core_dist_n_jobs"] == config.cpu_fallback_jobs


def test_fit_hdbscan_falls_back_to_cpu_on_gpu_oom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExplodingHDBSCAN:
        def __init__(self, **kwargs: object) -> None:
            pass

        def fit(self, vectors: np.ndarray) -> None:
            raise MemoryError("simulated GPU OOM")

    def fake_hdbscan_classes(device: str) -> type:
        return ExplodingHDBSCAN if device == "gpu" else hdbscan_cpu_module.HDBSCAN

    monkeypatch.setattr(hdbscan_models_module, "hdbscan_classes", fake_hdbscan_classes)
    monkeypatch.setattr(hdbscan_models_module, "_free_gpu_memory", lambda: None)

    vectors = _synthetic_blobs()
    config = _clustering_config(min_cluster_size=20)

    model = fit_hdbscan(vectors, config, device="gpu")

    assert isinstance(model, hdbscan_cpu_module.HDBSCAN)
    assert hasattr(model, "labels_")


def test_fit_hdbscan_reraises_non_oom_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class BrokenHDBSCAN:
        def __init__(self, **kwargs: object) -> None:
            pass

        def fit(self, vectors: np.ndarray) -> None:
            raise RuntimeError("totally unrelated failure")

    monkeypatch.setattr(
        hdbscan_models_module,
        "hdbscan_classes",
        lambda device: BrokenHDBSCAN if device == "gpu" else hdbscan_cpu_module.HDBSCAN,
    )

    vectors = _synthetic_blobs()
    config = _clustering_config(min_cluster_size=20)

    with pytest.raises(RuntimeError, match="unrelated"):
        fit_hdbscan(vectors, config, device="gpu")
