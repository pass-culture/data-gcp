import torch
from loguru import logger
from sentence_transformers import SentenceTransformer
from src.constants import HF_TOKEN_SECRET_NAME, MAX_SEQ_LENGTH
from src.gcp_secrets import get_secret


def _bf16_supported() -> bool:
    """True if the current CUDA device natively supports bfloat16 (Ampere+)."""
    major, _ = torch.cuda.get_device_capability(0)
    return major >= 8


def _resolve_precision(gpu_count: int) -> torch.dtype:
    """Resolve the torch dtype: bfloat16 on Ampere+ GPUs, else float32.
    float16 is deliberately never used (Gemma overflows and produces NaN).
    """
    if gpu_count != 0 and _bf16_supported():
        logger.info("GPU supports bfloat16; using bfloat16 precision")
        return torch.bfloat16

    logger.info("GPU does not support bfloat16 or no GPU: using float32 precision")
    return torch.float32


def load_encoder(encoder_name: str, gpu_count: int) -> SentenceTransformer:
    """Load one encoder with the appropriate precision and capped seq length.

    Args:
        encoder_name: HuggingFace model name or path.
        gpu_count: Number of available GPUs.

    Returns:
        The loaded SentenceTransformer.
    """
    token = get_secret(HF_TOKEN_SECRET_NAME)
    precision = _resolve_precision(gpu_count)

    # With >1 GPU the multi-process pool loads a full model copy onto every GPU,
    # so keep the main-process copy on CPU to avoid two copies on GPU 0.
    device = "cpu" if gpu_count > 1 else None

    logger.info(
        f"Loading encoder: {encoder_name} (precision={precision}, "
        f"main-process device={device or 'auto'})"
    )
    encoder = SentenceTransformer(
        encoder_name,
        token=token,
        device=device,
        model_kwargs={"torch_dtype": precision},
    )
    encoder.max_seq_length = MAX_SEQ_LENGTH
    return encoder


def start_pool(encoder: SentenceTransformer, gpu_count: int) -> dict | None:
    """Start a multi-process encoding pool if >1 GPU is available, else ``None``
    (single-device encoding is used).
    """
    if gpu_count <= 1:
        return None
    logger.info(f"Starting multi-GPU pool on {gpu_count} GPUs")
    return encoder.start_multi_process_pool()


def stop_pool(encoder: SentenceTransformer, pool: dict | None) -> None:
    """Shut down a pool started by ``start_pool`` (no-op if ``pool`` is None)."""
    if pool is not None:
        logger.info("Stopping multi-GPU pool")
        encoder.stop_multi_process_pool(pool)
