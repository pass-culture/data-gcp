"""Unit tests for prompt_building.py."""

from unittest.mock import MagicMock

import pandas as pd
import pytest
from config import CategoryFilter, FilterCondition, FilterGroup, Vector
from prompt_building import (
    LongPromptTracker,
    _build_prompts,
    _category_filter_mask,
    _find_long_prompts,
    _is_missing,
)


def _simple_filter(column: str, values=None, prefix=None) -> CategoryFilter:
    """Build a CategoryFilter matching a single condition (the common case:
    an any_of with one group holding one condition)."""
    return CategoryFilter(
        any_of=[
            FilterGroup(
                conditions=[
                    FilterCondition(column=column, values=values, prefix=prefix)
                ]
            )
        ]
    )


# ---------------------------------------------------------------------------
# Prompt building tests
# ---------------------------------------------------------------------------
class TestBuildPrompts:
    def make_vector(self, features):
        return Vector(name="test", features=features, encoder_name="model")

    def test_basic(self):
        df = pd.DataFrame({"x": ["hello"], "y": ["world"]})
        prompts = _build_prompts(df, self.make_vector(["x", "y"]))
        assert prompts == ["x : hello\ny : world"]

    def test_null_feature_skipped(self):
        df = pd.DataFrame({"x": ["hello"], "y": [None]})
        prompts = _build_prompts(df, self.make_vector(["x", "y"]))
        assert prompts == ["x : hello"]

    def test_all_null_produces_empty_string(self):
        # The empty prompt is kept in place so the result stays row-aligned
        # with the input; embed_dataframe is what drops the item later.
        df = pd.DataFrame({"x": [None], "y": [None]})
        prompts = _build_prompts(df, self.make_vector(["x", "y"]))
        assert prompts == [""]

    def test_empty_prompt_stays_in_position(self):
        # An all-null row in the middle must keep its slot so the list stays
        # aligned row-for-row with the DataFrame.
        df = pd.DataFrame({"x": ["hello", None, "world"]})
        prompts = _build_prompts(df, self.make_vector(["x"]))
        assert prompts == ["x : hello", "", "x : world"]

    def test_no_double_spaces_with_middle_null(self):
        df = pd.DataFrame({"a": ["v1"], "b": [None], "c": ["v3"]})
        prompts = _build_prompts(df, self.make_vector(["a", "b", "c"]))
        assert "\n\n" not in prompts[0]
        assert prompts[0] == "a : v1\nc : v3"

    def test_multiple_rows(self):
        df = pd.DataFrame({"x": ["a", "b", "c"]})
        prompts = _build_prompts(df, self.make_vector(["x"]))
        assert len(prompts) == 3
        assert prompts[1] == "x : b"

    def test_labels_override_column_names(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "author_concat": ["Herbert"]})
        vector = Vector(
            name="test",
            features=["offer_name", "author_concat"],
            encoder_name="model",
            labels={"offer_name": "titre", "author_concat": "auteur / artiste"},
        )
        prompts = _build_prompts(df, vector)
        assert prompts == ["titre : Dune\nauteur / artiste : Herbert"]

    def test_unmapped_feature_falls_back_to_column_name(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "category_id": ["LIVRE"]})
        vector = Vector(
            name="test",
            features=["offer_name", "category_id"],
            encoder_name="model",
            labels={"offer_name": "titre"},
        )
        prompts = _build_prompts(df, vector)
        assert prompts == ["titre : Dune\ncategory_id : LIVRE"]


# ---------------------------------------------------------------------------
# _is_missing: null check safe for JSON list/dict values
# ---------------------------------------------------------------------------
class TestIsMissing:
    def test_none_is_missing(self):
        assert _is_missing(None) is True

    def test_nan_is_missing(self):
        assert _is_missing(float("nan")) is True

    def test_string_is_not_missing(self):
        assert _is_missing("hello") is False

    def test_empty_string_is_not_missing(self):
        assert _is_missing("") is False

    def test_list_is_not_missing(self):
        # pd.notna(a_list) vectorizes elementwise and raises when used as a
        # bool; _is_missing must treat a non-empty list as present without
        # touching pd.notna on the raw value.
        assert _is_missing(["DRAMA", "ACTION"]) is False

    def test_empty_list_is_not_missing(self):
        assert _is_missing([]) is False

    def test_dict_is_not_missing(self):
        assert _is_missing({"gtl1": "roman"}) is False


# ---------------------------------------------------------------------------
# Prompt template tests
# ---------------------------------------------------------------------------
class TestPromptTemplate:
    def test_renders_template(self):
        df = pd.DataFrame(
            {"offer_name": ["Dune"], "offer_description": ["A desert planet"]}
        )
        vector = Vector(
            name="test",
            features=["offer_name", "offer_description"],
            encoder_name="model",
            prompt_template='Title: "{offer_name}". Description: {offer_description}.',
        )
        prompts = _build_prompts(df, vector)
        assert prompts == ['Title: "Dune". Description: A desert planet.']

    def test_missing_value_renders_as_empty_not_none(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "offer_description": [None]})
        vector = Vector(
            name="test",
            features=["offer_name", "offer_description"],
            encoder_name="model",
            prompt_template="{offer_name} - {offer_description}",
        )
        prompts = _build_prompts(df, vector)
        assert prompts == ["Dune - "]
        assert "None" not in prompts[0]

    def test_all_null_row_produces_empty_string(self):
        df = pd.DataFrame({"offer_name": [None], "offer_description": [None]})
        vector = Vector(
            name="test",
            features=["offer_name", "offer_description"],
            encoder_name="model",
            prompt_template="{offer_name} - {offer_description}",
        )
        prompts = _build_prompts(df, vector)
        assert prompts == [""]

    def test_template_referencing_undeclared_field_raises(self):
        df = pd.DataFrame({"offer_name": ["Dune"]})
        vector = Vector(
            name="test",
            features=["offer_name"],
            encoder_name="model",
            prompt_template="{offer_name} by {author}",
        )
        with pytest.raises(ValueError, match="unknown field"):
            _build_prompts(df, vector)

    def test_template_with_preprocessors_applied_first(self):
        df = pd.DataFrame({"offer_name": ["  Dune   Messiah  "]})
        vector = Vector(
            name="test",
            features=["offer_name"],
            encoder_name="model",
            prompt_template="Title: {offer_name}",
            preprocessors={"offer_name": "normalize_whitespace"},
        )
        prompts = _build_prompts(df, vector)
        assert prompts == ["Title: Dune Messiah"]

    def test_json_envelope_feature_with_movie_preprocessor(self):
        # extra_semantic_metadata holds the uniform envelope shape
        # ({"movies": {...}} / {"books": {...}}); the movies preprocessor
        # must extract its own "movies" entry and render blank (not crash or
        # render "None") when it's absent, e.g. for a non-movie row.
        df = pd.DataFrame(
            {
                "offer_name": ["Dune", "Book"],
                "extra_semantic_metadata": [
                    {"movies": {"genres": ["DRAMA", "ACTION"]}},
                    None,
                ],
            }
        )
        vector = Vector(
            name="test",
            features=["offer_name", "extra_semantic_metadata"],
            encoder_name="model",
            prompt_template="{offer_name} - Genres: {extra_semantic_metadata}",
            preprocessors={"extra_semantic_metadata": "format_movie_genres"},
        )
        prompts = _build_prompts(df, vector)
        assert prompts == ["Dune - Genres: DRAMA, ACTION", "Book - Genres: "]

    def test_json_envelope_feature_with_book_preprocessor(self):
        df = pd.DataFrame(
            {
                "offer_name": ["Dune"],
                "extra_semantic_metadata": [
                    {
                        "books": {
                            "gtl1": "roman",
                            "gtl2": "19eme siecle",
                            "gtl3": None,
                            "gtl4": None,
                        }
                    }
                ],
            }
        )
        vector = Vector(
            name="test",
            features=["offer_name", "extra_semantic_metadata"],
            encoder_name="model",
            prompt_template="{offer_name} - {extra_semantic_metadata}",
            preprocessors={"extra_semantic_metadata": "format_book_classification"},
        )
        prompts = _build_prompts(df, vector)
        assert prompts == ["Dune - niveau 1 : roman > niveau 2 : 19eme siecle"]


# ---------------------------------------------------------------------------
# _category_filter_mask: AND/OR composition
# ---------------------------------------------------------------------------
class TestCategoryFilterMask:
    def _df(self):
        return pd.DataFrame(
            {
                "item_id": ["p1", "p2", "p3", "o1"],
                "category_id": ["CINEMA", "LIVRE", "LIVRE", "CINEMA"],
                "subcategory_id": [
                    "SEANCE_CINE",
                    "LIVRE_PAPIER",
                    "LIVRE_NUMERIQUE",
                    "SEANCE_CINE",
                ],
            }
        )

    def test_all_of_only_is_pure_and(self):
        df = self._df()
        cf = CategoryFilter(
            all_of=[
                FilterCondition(column="item_id", prefix="p"),
                FilterCondition(column="category_id", values=["CINEMA"]),
            ]
        )
        mask = _category_filter_mask(df, cf)
        assert mask.tolist() == [True, False, False, False]

    def test_any_of_only_is_pure_or_of_and_groups(self):
        df = self._df()
        cf = CategoryFilter(
            any_of=[
                FilterGroup(
                    conditions=[
                        FilterCondition(column="category_id", values=["CINEMA"])
                    ]
                ),
                FilterGroup(
                    conditions=[
                        FilterCondition(column="category_id", values=["LIVRE"]),
                        FilterCondition(
                            column="subcategory_id", values=["LIVRE_PAPIER"]
                        ),
                    ]
                ),
            ]
        )
        mask = _category_filter_mask(df, cf)
        # p1/o1 (CINEMA) match group 1; p2 (LIVRE + LIVRE_PAPIER) matches
        # group 2; p3 (LIVRE + LIVRE_NUMERIQUE) matches neither group.
        assert mask.tolist() == [True, True, False, True]

    def test_all_of_and_any_of_are_anded_together(self):
        df = self._df()
        cf = CategoryFilter(
            all_of=[FilterCondition(column="item_id", prefix="p")],
            any_of=[
                FilterGroup(
                    conditions=[
                        FilterCondition(column="category_id", values=["CINEMA"])
                    ]
                )
            ],
        )
        mask = _category_filter_mask(df, cf)
        # o1 matches the any_of (CINEMA) but fails all_of (item_id prefix).
        assert mask.tolist() == [True, False, False, False]


# ---------------------------------------------------------------------------
# LongPromptTracker: end-of-job over-length prompt reporting
# ---------------------------------------------------------------------------
class TestLongPromptTracker:
    def test_record_accumulates_per_vector(self):
        tracker = LongPromptTracker()
        tracker.record("movies_content", "item-1")
        tracker.record("movies_content", "item-2")
        tracker.record("books_content", "item-3")
        assert tracker.long_item_ids == {
            "movies_content": ["item-1", "item-2"],
            "books_content": ["item-3"],
        }

    def test_default_max_tokens_matches_constant(self):
        from constants import MAX_SEQ_LENGTH

        assert LongPromptTracker().max_tokens == MAX_SEQ_LENGTH

    def test_log_summary_does_not_raise_when_empty(self):
        LongPromptTracker().log_summary()

    def test_log_summary_does_not_raise_when_populated(self):
        tracker = LongPromptTracker()
        tracker.record("v", "item-1")
        tracker.log_summary()


# ---------------------------------------------------------------------------
# _find_long_prompts: char-length pre-filter + exact token count
# ---------------------------------------------------------------------------
class TestFindLongPrompts:
    def _mock_encoder(self, max_seq_length, token_counts=None, prompts=None):
        encoder = MagicMock()
        encoder.max_seq_length = max_seq_length
        encoder.prompts = prompts or {}
        if token_counts is not None:
            encoder.tokenizer.return_value = {
                "input_ids": [[0] * n for n in token_counts]
            }
        return encoder

    def _vector(self, name="v", prompt_name=None):
        return Vector(
            name=name, features=["a"], encoder_name="model", prompt_name=prompt_name
        )

    def test_short_prompts_skip_tokenizer_entirely(self):
        # Nothing exceeds the cheap character pre-filter (2x max_seq_length
        # chars), so the tokenizer (expensive) must never be invoked.
        encoder = self._mock_encoder(max_seq_length=10)
        tracker = LongPromptTracker(max_tokens=10)
        _find_long_prompts(
            self._vector(), encoder, ["a", "b"], ["short one", "short two"], tracker
        )
        encoder.tokenizer.assert_not_called()
        assert tracker.long_item_ids == {}

    def test_candidate_under_real_token_limit_is_not_recorded(self):
        # Long enough in characters to pass the pre-filter, but the exact
        # tokenizer count is still under the limit -- must not be flagged
        # from character count alone.
        encoder = self._mock_encoder(max_seq_length=5, token_counts=[4])
        tracker = LongPromptTracker(max_tokens=5)
        long_prompt = "x" * 11  # > 2 * 5 chars
        _find_long_prompts(self._vector(), encoder, ["only"], [long_prompt], tracker)
        encoder.tokenizer.assert_called_once()
        assert tracker.long_item_ids == {}

    def test_candidate_over_real_token_limit_is_recorded(self):
        encoder = self._mock_encoder(max_seq_length=5, token_counts=[6])
        tracker = LongPromptTracker(max_tokens=5)
        long_prompt = "x" * 11
        _find_long_prompts(
            self._vector("movies_content"),
            encoder,
            ["item-1"],
            [long_prompt],
            tracker,
        )
        assert tracker.long_item_ids == {"movies_content": ["item-1"]}

    def test_only_candidates_are_tokenized_not_the_whole_batch(self):
        encoder = self._mock_encoder(max_seq_length=5, token_counts=[6])
        tracker = LongPromptTracker(max_tokens=5)
        prompts = ["short", "x" * 11]  # only index 1 clears the pre-filter
        _find_long_prompts(self._vector(), encoder, ["a", "b"], prompts, tracker)

        called_texts = encoder.tokenizer.call_args[0][0]
        assert called_texts == [prompts[1]]
        assert tracker.long_item_ids == {"v": ["b"]}

    def test_prompt_name_prefix_is_included_in_tokenized_text(self):
        encoder = self._mock_encoder(
            max_seq_length=5,
            token_counts=[6],
            prompts={"document": "title: none | text: "},
        )
        tracker = LongPromptTracker(max_tokens=5)
        long_prompt = "x" * 11
        vector = self._vector(prompt_name="document")

        _find_long_prompts(vector, encoder, ["item-1"], [long_prompt], tracker)

        called_texts = encoder.tokenizer.call_args[0][0]
        assert called_texts == ["title: none | text: " + long_prompt]

    def test_falls_back_to_tracker_max_tokens_when_encoder_has_no_max_seq_length(self):
        # An encoder without a real integer max_seq_length (e.g. a bare test
        # double) must not be treated as having no limit; falls back to
        # tracker.max_tokens rather than crashing or skipping the check.
        encoder = self._mock_encoder(max_seq_length=None, token_counts=[2049])
        tracker = LongPromptTracker(max_tokens=2048)
        long_prompt = "x" * 4100  # > 2 * 2048 chars

        _find_long_prompts(self._vector(), encoder, ["item-1"], [long_prompt], tracker)

        assert tracker.long_item_ids == {"v": ["item-1"]}
