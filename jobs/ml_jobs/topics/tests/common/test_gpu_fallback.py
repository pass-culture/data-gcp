from unittest.mock import MagicMock

import pytest

from src.common import gpu_fallback as gpu_fallback_module
from src.common.gpu_fallback import fit_with_gpu_fallback, is_gpu_oom_error


@pytest.mark.parametrize(
    "error",
    [
        MemoryError("cuda oom"),
        RuntimeError("CUDA error: out of memory"),
        RuntimeError("std::bad_alloc"),
        RuntimeError("cudaErrorMemoryAllocation: out of memory"),
    ],
)
def test_is_gpu_oom_error_true_for_known_signatures(error: Exception) -> None:
    assert is_gpu_oom_error(error)


@pytest.mark.parametrize(
    "error", [RuntimeError("some other failure"), ValueError("bad input")]
)
def test_is_gpu_oom_error_false_for_other_errors(error: Exception) -> None:
    assert not is_gpu_oom_error(error)


def test_fit_with_gpu_fallback_runs_cpu_directly_when_device_is_cpu() -> None:
    gpu_fit = MagicMock(return_value="gpu")

    result = fit_with_gpu_fallback("stage", gpu_fit, lambda: "cpu", device="cpu")

    assert result == "cpu"
    gpu_fit.assert_not_called()


def test_fit_with_gpu_fallback_uses_gpu_result_on_success() -> None:
    result = fit_with_gpu_fallback(
        "stage", lambda: "gpu-result", lambda: "cpu-result", device="gpu"
    )
    assert result == "gpu-result"


def test_fit_with_gpu_fallback_falls_back_to_cpu_on_oom_and_logs_clearly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warning_mock = MagicMock()
    monkeypatch.setattr(gpu_fallback_module.logger, "warning", warning_mock)

    def gpu_fit() -> str:
        raise MemoryError("out of memory")

    result = fit_with_gpu_fallback(
        "my-stage", gpu_fit, lambda: "cpu-result", device="gpu"
    )

    assert result == "cpu-result"
    warning_mock.assert_called_once()
    message = warning_mock.call_args[0][0]
    assert "my-stage" in message
    assert "out of memory" in message


def test_fit_with_gpu_fallback_frees_gpu_memory_before_cpu_fit() -> None:
    calls: list[str] = []

    def gpu_fit() -> str:
        raise MemoryError("oom")

    def free_gpu_memory() -> None:
        calls.append("freed")

    def cpu_fit() -> str:
        calls.append("cpu")
        return "result"

    result = fit_with_gpu_fallback(
        "stage", gpu_fit, cpu_fit, device="gpu", free_gpu_memory=free_gpu_memory
    )
    assert result == "result"
    assert calls == ["freed", "cpu"]


def test_fit_with_gpu_fallback_reraises_non_oom_runtime_error() -> None:
    def gpu_fit() -> None:
        raise RuntimeError("some unrelated failure")

    with pytest.raises(RuntimeError, match="unrelated"):
        fit_with_gpu_fallback("stage", gpu_fit, lambda: "cpu", device="gpu")


def test_fit_with_gpu_fallback_reraises_other_error_types() -> None:
    def gpu_fit() -> None:
        raise ValueError("bad input")

    with pytest.raises(ValueError, match="bad input"):
        fit_with_gpu_fallback("stage", gpu_fit, lambda: "cpu", device="gpu")
