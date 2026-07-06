import torch
from config import Vector
from constants import (
    HF_TOKEN_SECRET_NAME,
)
from gcp_secrets import get_secret
from loguru import logger
from sentence_transformers import SentenceTransformer


def _resolve_precision(gpu_count: int) -> str:
    """Resolve the precision to use for the run. Depends on GPU count and type

    Args:
        gpu_count: Number of available GPUs

    Returns:
        Precision torch dtype
    """
    if gpu_count != 0 & _bf16_supported():
        logger.info("GPU supports bfloat16; using bfloat16 precision")
        return torch.bfloat16

    return torch.float32


def _bf16_supported() -> bool:
    """True if the current CUDA device natively supports bfloat16 (Ampere+)."""
    major, _ = torch.cuda.get_device_capability(0)
    return major >= 8


def load_encoders(
    vectors: list[Vector], gpu_count: int
) -> dict[str, SentenceTransformer]:
    """Load each unique encoder once with the appropriate precision.

    Args:
        vectors: List of vector configurations
        gpu_count: Number of available GPUs

    Returns:
        Dictionary mapping encoder names to loaded SentenceTransformer instances
    """
    unique_encoder_names = {v.encoder_name for v in vectors}
    token = get_secret(HF_TOKEN_SECRET_NAME)
    precision = _resolve_precision(gpu_count)

    encoders = {}
    for name in unique_encoder_names:
        logger.info(f"Loading encoder: {name} (precision={precision})")
        encoders[name] = SentenceTransformer(
            name, token=token, model_kwargs={"torch_dtype": precision}
        )
    return encoders


def start_encoder_pools(
    encoders: dict[str, SentenceTransformer], gpu_count: int
) -> dict[str, object]:
    """Start one multi-process encoding pool per encoder, once for the whole run.

    Args:
        encoders: Pre-loaded encoders keyed by encoder name
        gpu_count: Number of available GPUs

    Returns:
        Mapping of encoder name -> multi-process pool (empty if single-device)
    """
    if gpu_count <= 1:
        return {}

    pools: dict[str, object] = {}
    for name, encoder in encoders.items():
        logger.info(f"Starting multi-GPU pool for encoder '{name}' on {gpu_count} GPUs")
        pools[name] = encoder.start_multi_process_pool()
    return pools


def stop_encoder_pools(
    encoders: dict[str, SentenceTransformer], pools: dict[str, object]
) -> None:
    """Shut down all multi-process encoding pools started by ``start_encoder_pools``."""
    for name, pool in pools.items():
        logger.info(f"Stopping multi-GPU pool for encoder '{name}'")
        encoders[name].stop_multi_process_pool(pool)
