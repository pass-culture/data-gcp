import torch
from config import Vector
from constants import HF_TOKEN_SECRET_NAME
from gcp_secrets import get_secret
from loguru import logger
from sentence_transformers import SentenceTransformer


def _resolve_precision(gpu_count: int) -> torch.dtype:
    """Resolve the torch dtype to use, based on GPU count and capability.

    Uses bfloat16 on Ampere+ GPUs, else float32. float16 is deliberately never
    used: Gemma models overflow in float16 and produce NaN embeddings.

    Args:
        gpu_count: Number of available GPUs

    Returns:
        Precision as a torch dtype
    """
    if gpu_count != 0 and _bf16_supported():
        logger.info("GPU supports bfloat16; using bfloat16 precision")
        return torch.bfloat16

    logger.info("GPU does not support bfloat16 or no GPU: using float32 precision")
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

    # With >1 GPU, the multi-process pool loads a full model copy onto every GPU.
    # Keep the main-process copy on CPU so GPU 0 does not hold two copies (OOM).
    device = "cpu" if gpu_count > 1 else None

    encoders = {}
    for name in unique_encoder_names:
        logger.info(
            f"Loading encoder: {name} (precision={precision}, "
            f"main-process device={device or 'auto'})"
        )
        encoder = SentenceTransformer(
            name,
            token=token,
            device=device,
            model_kwargs={"torch_dtype": precision},
        )
        encoders[name] = encoder
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
