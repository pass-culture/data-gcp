from typing import Optional

import torch
from config import Vector
from constants import HF_TOKEN_SECRET_NAME, MAX_SEQ_LENGTH
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


def load_encoder(vector: Vector, gpu_count: int) -> SentenceTransformer:
    """Load the vector's encoder with the appropriate precision.

    Args:
        vector: Vector configuration
        gpu_count: Number of available GPUs

    Returns:
        Loaded SentenceTransformer instance
    """
    token = get_secret(HF_TOKEN_SECRET_NAME)
    precision = _resolve_precision(gpu_count)

    # With >1 GPU, the multi-process pool loads a full model copy onto **every** GPU.
    # so we keep the main-process copy on CPU so GPU 0 does not hold two copies (risk of OOM).
    device = "cpu" if gpu_count > 1 else None

    logger.info(
        f"Loading encoder: {vector.encoder_name} (precision={precision}, "
        f"main-process device={device or 'auto'})"
    )
    encoder = SentenceTransformer(
        vector.encoder_name,
        token=token,
        device=device,
        model_kwargs={"torch_dtype": precision},
    )
    encoder.max_seq_length = MAX_SEQ_LENGTH
    return encoder


def start_encoder_pool(
    encoder: SentenceTransformer, gpu_count: int
) -> Optional[object]:
    """Start a multi-process encoding pool for the encoder, once for the whole run.
    If more than one GPU is not available, returns ``None`` (single-device encoding
    will be used).

    Args:
        encoder: Pre-loaded encoder
        gpu_count: Number of available GPUs

    Returns:
        Multi-process pool, or ``None`` if single-device
    """
    if gpu_count <= 1:
        return None

    logger.info(f"Starting multi-GPU pool for encoder on {gpu_count} GPUs")
    return encoder.start_multi_process_pool()


def stop_encoder_pool(encoder: SentenceTransformer, pool: Optional[object]) -> None:
    """Shut down the multi-process encoding pool started by ``start_encoder_pool``."""
    if pool is None:
        return
    logger.info("Stopping multi-GPU pool for encoder")
    encoder.stop_multi_process_pool(pool)
