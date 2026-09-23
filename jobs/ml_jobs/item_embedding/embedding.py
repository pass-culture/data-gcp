"""Turning prompts into embedding vectors: the single ``encode()`` call plus the
over-length-prompt detection that makes silent truncation visible.

This is the logic behind the ``embed`` step (``cli/embed.py``). Each run embeds
exactly one vector, so there is no cross-vector orchestration here.
"""

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
from config import Vector
from constants import BATCH_SIZE, MAX_SEQ_LENGTH
from loguru import logger
from sentence_transformers import SentenceTransformer


def encode(
    encoder: SentenceTransformer,
    prompts: list[str],
    prompt_name: Optional[str] = None,
    pool: object = None,
) -> np.ndarray:
    """Generate embeddings for a batch of prompts.

    Uses multi-GPU encoding when a pre-started ``pool`` is provided, otherwise
    single-device encoding. Batching within the call is delegated to
    SentenceTransformer.

    Args:
        encoder: Pre-loaded SentenceTransformer encoder.
        prompts: Prompts to embed.
        prompt_name: The encoder's named prompt to prepend (model-dependent).
        pool: Pre-started multi-process pool, or ``None`` for single-device.

    Returns:
        Numpy array of shape (n_prompts, embedding_dim).
    """
    encode_kwargs = {
        "convert_to_numpy": True,
        "show_progress_bar": False,
        "batch_size": BATCH_SIZE,
        "prompt_name": prompt_name,
        "normalize_embeddings": True,
    }

    if pool is not None:
        logger.info(
            f"Multi-GPU encoding {len(prompts)} prompts (batch_size={BATCH_SIZE})"
        )
        embeddings = encoder.encode(prompts, pool=pool, **encode_kwargs)
    else:
        logger.info(
            f"Single-device encoding {len(prompts)} prompts on {encoder.device} "
            f"(batch_size={BATCH_SIZE})"
        )
        embeddings = encoder.encode(prompts, **encode_kwargs)

    logger.info(f"Generated {len(embeddings)} embeddings with shape {embeddings.shape}")
    return embeddings


@dataclass
class LongPromptTracker:
    """Accumulates items whose prompt exceeded the encoder's token limit across
    a whole run (all chunks), so they're reported once as an end-of-job summary
    instead of scattered per chunk.
    """

    max_tokens: int = MAX_SEQ_LENGTH
    long_item_ids: list = field(default_factory=list)

    def record(self, item_id: object) -> None:
        self.long_item_ids.append(item_id)

    def log_summary(self) -> None:
        if not self.long_item_ids:
            logger.info("No prompts exceeded the token limit.")
            return
        logger.warning(
            f"{len(self.long_item_ids)} item(s) had a prompt exceeding the token "
            f"limit and were silently truncated by the encoder. Item ids:\n"
            f"{self.long_item_ids}"
        )


def _resolve_prompt_prefix(encoder: SentenceTransformer, vector: Vector) -> str:
    """Returns the fixed prefix ``encoder.encode()`` prepends for
    ``vector.prompt_name`` (e.g. embeddinggemma's "document" prompt), or ``""``
    if none is configured. Mirrors what SentenceTransformer does internally, so
    the text tokenized to check length matches what is actually sent.
    """
    if vector.prompt_name is None:
        logger.info("No prompt name specified, using empty.")
        return ""
    return encoder.prompts.get(vector.prompt_name, "")


def find_long_prompts(
    vector: Vector,
    encoder: SentenceTransformer,
    item_ids: list,
    prompts: list[str],
    tracker: LongPromptTracker,
) -> None:
    """Flags prompts likely to exceed the encoder's max sequence length.

    SentenceTransformers silently truncates any prompt longer than
    ``max_seq_length`` (dropping the end), so truncation is otherwise invisible.
    A cheap character-length pre-filter (> 2x the token limit) shortlists
    candidates, then only those are tokenized (batched, no truncation) for an
    exact count. Confirmed over-limit prompts are logged and recorded.
    """
    encoder_max_seq_length = getattr(encoder, "max_seq_length", None)
    max_tokens = (
        encoder_max_seq_length
        if isinstance(encoder_max_seq_length, int)
        else tracker.max_tokens
    )
    length_threshold = 2 * max_tokens

    candidate_indices = [
        i for i, prompt in enumerate(prompts) if len(prompt) > length_threshold
    ]
    if not candidate_indices:
        return

    prefix = _resolve_prompt_prefix(encoder, vector)
    candidate_texts = [prefix + prompts[i] for i in candidate_indices]
    token_counts = [
        len(input_ids)
        for input_ids in encoder.tokenizer(
            candidate_texts, truncation=False, padding=False
        )["input_ids"]
    ]

    for i, token_count in zip(candidate_indices, token_counts):
        if token_count > max_tokens:
            item_id = item_ids[i]
            logger.warning(
                f"Vector '{vector.name}': item '{item_id}' prompt has "
                f"{token_count} tokens, exceeding max_seq_length={max_tokens}; "
                f"it will be silently truncated by the encoder."
            )
            tracker.record(item_id)
