"""Shared GPU-out-of-memory detection and CPU-refit helper (spec section
7.3's OOM-fallback rule, applied symmetrically to both ``reduction`` and
``clustering``): a GPU ``MemoryError``, or a ``RuntimeError`` whose message
contains one of a few known CUDA OOM signatures, means "retry on CPU", with
a clear log line naming what failed and why. Any other error is re-raised
untouched.
"""

from collections.abc import Callable

from loguru import logger

OOM_MESSAGE_MARKERS = ("out of memory", "bad_alloc", "cudaErrorMemoryAllocation")


def is_gpu_oom_error(error: Exception) -> bool:
    if isinstance(error, MemoryError):
        return True
    if isinstance(error, RuntimeError):
        return any(marker in str(error) for marker in OOM_MESSAGE_MARKERS)
    return False


def fit_with_gpu_fallback[FitResult](
    stage: str,
    gpu_fit: Callable[[], FitResult],
    cpu_fit: Callable[[], FitResult],
    *,
    device: str,
    free_gpu_memory: Callable[[], None] | None = None,
) -> FitResult:
    """Run ``gpu_fit`` when ``device == "gpu"``; on a GPU out-of-memory
    error, log a clear warning naming ``stage`` and the error, free the GPU
    memory pool (if ``free_gpu_memory`` is given), then run ``cpu_fit``
    instead. Any other error from ``gpu_fit`` is re-raised untouched. When
    ``device != "gpu"``, runs ``cpu_fit`` directly without importing any GPU
    library.
    """
    if device != "gpu":
        return cpu_fit()

    try:
        return gpu_fit()
    except (MemoryError, RuntimeError) as error:
        if not is_gpu_oom_error(error):
            raise
        logger.warning(
            f"{stage}: GPU fit ran out of memory ({error}); falling back to CPU"
        )
        if free_gpu_memory is not None:
            free_gpu_memory()
        return cpu_fit()
