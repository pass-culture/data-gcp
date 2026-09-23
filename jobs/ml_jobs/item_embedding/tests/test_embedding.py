"""Unit tests for embedding.py (encode + long-prompt tracking)."""

from unittest.mock import MagicMock

import numpy as np
from config import Vector
from embedding import LongPromptTracker, encode, find_long_prompts


def _vector(name="v", prompt_name=None):
    return Vector(
        name=name, features=["a"], encoder_name="model", prompt_name=prompt_name
    )


class TestEncode:
    def test_single_device_encode(self):
        encoder = MagicMock()
        encoder.device = "cpu"
        encoder.encode.return_value = np.array([[1.0, 2.0], [3.0, 4.0]])

        out = encode(encoder, ["p1", "p2"], prompt_name="document")

        assert out.shape == (2, 2)
        (called_prompts,), kwargs = encoder.encode.call_args
        assert called_prompts == ["p1", "p2"]
        assert kwargs["prompt_name"] == "document"
        assert kwargs["normalize_embeddings"] is True
        assert "pool" not in kwargs

    def test_multi_gpu_encode_passes_pool(self):
        encoder = MagicMock()
        encoder.device = "cpu"
        encoder.encode.return_value = np.array([[1.0, 2.0]])
        pool = {"fake": "pool"}

        encode(encoder, ["p1"], prompt_name=None, pool=pool)

        _, kwargs = encoder.encode.call_args
        assert kwargs["pool"] is pool


class TestLongPromptTracker:
    def test_record_accumulates(self):
        tracker = LongPromptTracker()
        tracker.record("item-1")
        tracker.record("item-2")
        assert tracker.long_item_ids == ["item-1", "item-2"]

    def test_default_max_tokens_matches_constant(self):
        from constants import MAX_SEQ_LENGTH

        assert LongPromptTracker().max_tokens == MAX_SEQ_LENGTH

    def test_log_summary_empty_and_populated_do_not_raise(self):
        LongPromptTracker().log_summary()
        tracker = LongPromptTracker()
        tracker.record("item-1")
        tracker.log_summary()


class TestFindLongPrompts:
    def _encoder(self, max_seq_length, token_counts=None, prompts=None):
        encoder = MagicMock()
        encoder.max_seq_length = max_seq_length
        encoder.prompts = prompts or {}
        if token_counts is not None:
            encoder.tokenizer.return_value = {
                "input_ids": [[0] * n for n in token_counts]
            }
        return encoder

    def test_short_prompts_skip_tokenizer(self):
        encoder = self._encoder(max_seq_length=10)
        tracker = LongPromptTracker(max_tokens=10)
        find_long_prompts(_vector(), encoder, ["a", "b"], ["short", "also"], tracker)
        encoder.tokenizer.assert_not_called()
        assert tracker.long_item_ids == []

    def test_candidate_under_real_limit_not_recorded(self):
        encoder = self._encoder(max_seq_length=5, token_counts=[4])
        tracker = LongPromptTracker(max_tokens=5)
        find_long_prompts(_vector(), encoder, ["only"], ["x" * 11], tracker)
        encoder.tokenizer.assert_called_once()
        assert tracker.long_item_ids == []

    def test_candidate_over_real_limit_recorded(self):
        encoder = self._encoder(max_seq_length=5, token_counts=[6])
        tracker = LongPromptTracker(max_tokens=5)
        find_long_prompts(_vector(), encoder, ["item-1"], ["x" * 11], tracker)
        assert tracker.long_item_ids == ["item-1"]

    def test_only_candidates_tokenized(self):
        encoder = self._encoder(max_seq_length=5, token_counts=[6])
        tracker = LongPromptTracker(max_tokens=5)
        prompts = ["short", "x" * 11]  # only index 1 clears the pre-filter
        find_long_prompts(_vector(), encoder, ["a", "b"], prompts, tracker)
        assert encoder.tokenizer.call_args[0][0] == [prompts[1]]
        assert tracker.long_item_ids == ["b"]

    def test_prompt_name_prefix_included_in_tokenized_text(self):
        encoder = self._encoder(
            max_seq_length=5,
            token_counts=[6],
            prompts={"document": "title: none | text: "},
        )
        tracker = LongPromptTracker(max_tokens=5)
        find_long_prompts(
            _vector(prompt_name="document"), encoder, ["item-1"], ["x" * 11], tracker
        )
        assert encoder.tokenizer.call_args[0][0] == ["title: none | text: " + "x" * 11]

    def test_falls_back_to_tracker_max_tokens_when_encoder_has_none(self):
        encoder = self._encoder(max_seq_length=None, token_counts=[2049])
        tracker = LongPromptTracker(max_tokens=2048)
        find_long_prompts(_vector(), encoder, ["item-1"], ["x" * 4100], tracker)
        assert tracker.long_item_ids == ["item-1"]
