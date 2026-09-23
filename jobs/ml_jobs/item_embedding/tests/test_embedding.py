"""Unit tests for the item_embedding job."""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import yaml
from config import (
    CategoryFilter,
    FilterCondition,
    FilterGroup,
    Vector,
    _load_config,
    parse_vectors,
)
from embedding import _batch_encode, _PendingVectorEmbed, embed_dataframe
from prompt_building import LongPromptTracker


# ---------------------------------------------------------------------------
# Vector model tests
# ---------------------------------------------------------------------------
class TestVector:
    def test_valid_vector(self):
        v = Vector(name="test", features=["a", "b"], encoder_name="model/name")
        assert v.name == "test"
        assert v.prompt_name is None

    def test_vector_with_prompt_name(self):
        v = Vector(
            name="test",
            features=["a"],
            encoder_name="model/name",
            prompt_name="STS",
        )
        assert v.prompt_name == "STS"

    def test_vector_missing_required_field(self):
        with pytest.raises(Exception):
            Vector(name="test", features=["a"])  # missing encoder_name


# ---------------------------------------------------------------------------
# Config loading tests
# ---------------------------------------------------------------------------
class TestLoadConfig:
    def test_load_missing_file(self, tmp_path):
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(FileNotFoundError):
                _load_config("nonexistent")

    def test_load_invalid_yaml(self, tmp_path):
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text(": :\n  - :\n  invalid", encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(yaml.YAMLError):
                _load_config("bad")

    def test_load_valid_config(self, tmp_path):
        config_content = {
            "vectors": [
                {
                    "name": "test_vec",
                    "features": ["col_a"],
                    "encoder_name": "test/model",
                }
            ]
        }
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump(config_content), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            config = _load_config("test")
        assert "vectors" in config

    def test_load_config_missing_vectors_key(self, tmp_path):
        config_file = tmp_path / "no_vectors.yaml"
        config_file.write_text(yaml.dump({"other_key": 123}), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="missing required keys"):
                _load_config("no_vectors")


class TestParseVectors:
    def test_parse_valid(self, tmp_path):
        config_content = {
            "vectors": [
                {
                    "name": "v1",
                    "features": ["a", "b"],
                    "encoder_name": "model/x",
                }
            ]
        }
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump(config_content), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            vectors = parse_vectors("test")
        assert len(vectors) == 1
        assert vectors[0].name == "v1"

    def test_parse_empty_vectors(self, tmp_path):
        config_file = tmp_path / "empty.yaml"
        config_file.write_text(yaml.dump({"vectors": []}), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="No vectors configured"):
                parse_vectors("empty")

    def test_parse_no_vectors_key(self, tmp_path):
        config_file = tmp_path / "no_vectors.yaml"
        config_file.write_text(yaml.dump({}), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="missing required keys"):
                parse_vectors("no_vectors")

    def test_parse_invalid_vectors_type(self, tmp_path):
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text(yaml.dump({"vectors": "not_a_list"}), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="must be a list"):
                parse_vectors("invalid")


# ---------------------------------------------------------------------------
# End-to-end embed_dataframe tests
# ---------------------------------------------------------------------------
class TestEmbedDataframe:
    def test_end_to_end(self):
        # Mock encoder that returns deterministic embeddings
        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.return_value = np.array(
            [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        )

        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "c"],
                "content_hash": ["h1", "h2", "h3"],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        vectors = [Vector(name="emb", features=["name"], encoder_name="test/model")]
        encoders = {"test/model": mock_encoder}

        result = embed_dataframe(df, vectors, encoders)

        assert "item_id" in result.columns
        assert "content_hash" in result.columns
        assert "emb" in result.columns
        assert len(result) == 3

        # Verify encoder.encode was called
        assert mock_encoder.encode.called

    def test_all_null_row_is_skipped_and_not_embedded(self):
        # Only the two non-empty rows should be embedded; the all-null row
        # must not be sent to the encoder and must be dropped from the output,
        # so no null vector ever reaches the parquet.
        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.return_value = np.array([[1.0, 2.0], [5.0, 6.0]])

        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "c"],
                "content_hash": ["h1", "h2", "h3"],
                "name": ["Alice", None, "Charlie"],
            }
        )
        vectors = [Vector(name="emb", features=["name"], encoder_name="test/model")]
        encoders = {"test/model": mock_encoder}

        result = embed_dataframe(df, vectors, encoders)

        # The all-null item ("b") is excluded; survivors keep their embeddings.
        assert result["item_id"].tolist() == ["a", "c"]
        assert result["emb"].tolist() == [[1.0, 2.0], [5.0, 6.0]]
        assert result["emb"].notna().all()

        # The empty prompt was never passed to the encoder.
        (called_prompts,), _ = mock_encoder.encode.call_args
        assert called_prompts == ["name : Alice", "name : Charlie"]

    def test_each_item_keeps_its_own_embedding(self):
        # The embedding an item ends up with must be the one built from *that*
        # item's prompt, even when a middle item is dropped and the input has a
        # non-default index.
        def encode_from_prompts(prompts, **kwargs):
            # Turn each prompt into a distinct, content-derived vector so any
            # mismatch between items and embeddings would show up.
            return np.array([[float(len(p)), float(ord(p[-1]))] for p in prompts])

        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.side_effect = encode_from_prompts

        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "c", "d"],
                "content_hash": ["h1", "h2", "h3", "h4"],
                "name": ["Alice", "Bob", None, "Dana"],
            },
            index=[10, 20, 30, 40],  # non-default index must not break alignment
        )
        vectors = [Vector(name="emb", features=["name"], encoder_name="test/model")]
        encoders = {"test/model": mock_encoder}

        result = embed_dataframe(df, vectors, encoders)

        # "c" is dropped; the survivors keep their order and identity.
        assert result["item_id"].tolist() == ["a", "b", "d"]

        # Each surviving item maps to the embedding built from its own prompt.
        expected = {
            "a": [float(len("name : Alice")), float(ord("e"))],
            "b": [float(len("name : Bob")), float(ord("b"))],
            "d": [float(len("name : Dana")), float(ord("a"))],
        }
        for item_id, embedding in zip(result["item_id"], result["emb"]):
            assert embedding == expected[item_id]


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
# FilterCondition / CategoryFilter config tests
# ---------------------------------------------------------------------------
class TestFilterCondition:
    def test_with_values(self):
        c = FilterCondition(column="category_id", values=["CINEMA"])
        assert c.values == ["CINEMA"]
        assert c.prefix is None

    def test_with_prefix(self):
        c = FilterCondition(column="item_id", prefix="product")
        assert c.values is None
        assert c.prefix == "product"

    def test_requires_values_or_prefix(self):
        with pytest.raises(ValueError, match="exactly one of"):
            FilterCondition(column="item_id")

    def test_rejects_both_values_and_prefix(self):
        with pytest.raises(ValueError, match="exactly one of"):
            FilterCondition(column="item_id", values=["CINEMA"], prefix="product")


class TestCategoryFilter:
    def test_vector_without_category_filter_defaults_to_none(self):
        v = Vector(name="test", features=["a"], encoder_name="model")
        assert v.category_filter is None

    def test_vector_with_category_filter(self):
        v = Vector(
            name="test",
            features=["a"],
            encoder_name="model",
            category_filter=_simple_filter("category_id", values=["CINEMA"]),
        )
        assert v.category_filter is not None
        assert v.category_filter.any_of[0].conditions[0].values == ["CINEMA"]

    def test_all_of_and_any_of_can_combine(self):
        cf = CategoryFilter(
            all_of=[FilterCondition(column="item_id", prefix="product")],
            any_of=[
                FilterGroup(
                    conditions=[
                        FilterCondition(column="category_id", values=["CINEMA"])
                    ]
                )
            ],
        )
        assert cf.all_of[0].prefix == "product"
        assert cf.any_of[0].conditions[0].values == ["CINEMA"]

    def test_requires_all_of_or_any_of(self):
        with pytest.raises(ValueError, match="at least one of"):
            CategoryFilter()


class TestPreprocessorValidation:
    def test_unknown_preprocessor_raises(self):
        with pytest.raises(ValueError, match="Unknown preprocessor"):
            Vector(
                name="test",
                features=["a"],
                encoder_name="model",
                preprocessors={"a": "does_not_exist"},
            )

    def test_known_preprocessor_is_accepted(self):
        v = Vector(
            name="test",
            features=["a"],
            encoder_name="model",
            preprocessors={"a": "normalize_whitespace"},
        )
        assert v.preprocessors == {"a": "normalize_whitespace"}


# ---------------------------------------------------------------------------
# embed_dataframe: category-scoped vectors mixed with global vectors
# ---------------------------------------------------------------------------
class TestEmbedDataframeCategoryScoped:
    def _mock_encoder(self):
        encoder = MagicMock()
        encoder.device = "cpu"

        def encode(prompts, **kwargs):
            return np.array([[float(len(p)), float(i)] for i, p in enumerate(prompts)])

        encoder.encode.side_effect = encode
        return encoder

    def test_mixed_global_and_scoped_vector(self):
        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "c", "d"],
                "content_hash": ["h1", "h2", "h3", "h4"],
                "category_id": ["CINEMA", "LIVRE", "CINEMA", "LIVRE"],
                "name": ["Alice", "Bob", "Charlie", "Dana"],
                "title": ["Movie A", "Book B", "Movie C", "Book D"],
            }
        )
        global_vector = Vector(
            name="semantic_content", features=["name"], encoder_name="global/model"
        )
        movie_vector = Vector(
            name="movies_content",
            features=["title"],
            encoder_name="movie/model",
            category_filter=_simple_filter("category_id", values=["CINEMA"]),
        )
        encoders = {
            "global/model": self._mock_encoder(),
            "movie/model": self._mock_encoder(),
        }

        result = embed_dataframe(df, [global_vector, movie_vector], encoders)

        assert sorted(result["item_id"].tolist()) == ["a", "b", "c", "d"]
        assert result["semantic_content"].notna().all()

        by_id = result.set_index("item_id")
        is_na = by_id["movies_content"].isna()
        assert not is_na["a"]
        assert not is_na["c"]
        assert is_na["b"]
        assert is_na["d"]

        # content_hash must be preserved for every item regardless of which
        # vector(s) it matched.
        assert by_id.loc["a", "content_hash"] == "h1"
        assert by_id.loc["d", "content_hash"] == "h4"

    def test_prefix_filter_matches_item_id_prefix(self):
        # SQL equivalent: LEFT(item_id, LEN('product')) = 'product'
        df = pd.DataFrame(
            {
                "item_id": ["product-1", "product-2", "offer-1"],
                "content_hash": ["h1", "h2", "h3"],
                "title": ["Product One", "Product Two", "Offer One"],
            }
        )
        product_vector = Vector(
            name="product_content",
            features=["title"],
            encoder_name="model",
            category_filter=_simple_filter("item_id", prefix="product"),
        )
        encoders = {"model": self._mock_encoder()}

        result = embed_dataframe(df, [product_vector], encoders)

        assert sorted(result["item_id"].tolist()) == ["product-1", "product-2"]

    def test_all_of_and_any_of_combine_with_and(self):
        # all_of (item_id prefix "product") AND any_of (category CINEMA OR
        # (category LIVRE AND subcategory in [LIVRE_PAPIER])).
        df = pd.DataFrame(
            {
                "item_id": [
                    "product-1",  # CINEMA, matches any_of via first group
                    "product-2",  # LIVRE + matching subcategory, matches 2nd group
                    "product-3",  # LIVRE but wrong subcategory, matches no group
                    "offer-1",  # CINEMA but fails all_of (not "product" prefix)
                ],
                "content_hash": ["h1", "h2", "h3", "h4"],
                "category_id": ["CINEMA", "LIVRE", "LIVRE", "CINEMA"],
                "subcategory_id": [
                    "SEANCE_CINE",
                    "LIVRE_PAPIER",
                    "LIVRE_NUMERIQUE",
                    "SEANCE_CINE",
                ],
                "title": ["Movie A", "Book B", "Book C", "Movie D"],
            }
        )
        vector = Vector(
            name="products_content",
            features=["title"],
            encoder_name="model",
            category_filter=CategoryFilter(
                all_of=[FilterCondition(column="item_id", prefix="product")],
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
                ],
            ),
        )
        encoders = {"model": self._mock_encoder()}

        result = embed_dataframe(df, [vector], encoders)

        assert sorted(result["item_id"].tolist()) == ["product-1", "product-2"]

    def test_category_only_config_drops_unmatched_items(self):
        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "e"],
                "content_hash": ["h1", "h2", "h5"],
                "category_id": ["CINEMA", "LIVRE", "MUSIQUE"],
                "title": ["Movie A", "Book B", "Album E"],
                "author": [None, "Author B", None],
            }
        )
        movie_vector = Vector(
            name="movies_content",
            features=["title"],
            encoder_name="movie/model",
            category_filter=_simple_filter("category_id", values=["CINEMA"]),
        )
        book_vector = Vector(
            name="books_content",
            features=["title", "author"],
            encoder_name="book/model",
            category_filter=_simple_filter("category_id", values=["LIVRE"]),
        )
        encoders = {
            "movie/model": self._mock_encoder(),
            "book/model": self._mock_encoder(),
        }

        result = embed_dataframe(df, [movie_vector, book_vector], encoders)

        # "e" (MUSIQUE) matches neither scoped vector, so it must be dropped
        # entirely rather than kept with all-null vector columns.
        assert sorted(result["item_id"].tolist()) == ["a", "b"]

    def test_duplicate_item_id_raises(self):
        df = pd.DataFrame(
            {
                "item_id": ["a", "a"],
                "content_hash": ["h1", "h2"],
                "name": ["Alice", "Alice2"],
            }
        )
        vector = Vector(
            name="semantic_content", features=["name"], encoder_name="model"
        )
        with pytest.raises(ValueError, match="duplicate item_id"):
            embed_dataframe(df, [vector], {"model": self._mock_encoder()})

    def test_column_order_matches_config_declaration_order(self):
        df = pd.DataFrame(
            {
                "item_id": ["a"],
                "content_hash": ["h1"],
                "category_id": ["CINEMA"],
                "title": ["Movie A"],
                "name": ["Alice"],
            }
        )
        scoped_vector = Vector(
            name="movies_content",
            features=["title"],
            encoder_name="movie/model",
            category_filter=_simple_filter("category_id", values=["CINEMA"]),
        )
        global_vector = Vector(
            name="semantic_content", features=["name"], encoder_name="global/model"
        )
        encoders = {
            "movie/model": self._mock_encoder(),
            "global/model": self._mock_encoder(),
        }

        result = embed_dataframe(df, [scoped_vector, global_vector], encoders)

        assert list(result.columns) == [
            "item_id",
            "content_hash",
            "movies_content",
            "semantic_content",
        ]


# ---------------------------------------------------------------------------
# Encoder batching: vectors sharing (encoder_name, prompt_name) are merged
# into a single encode() call to avoid GPU underutilization when a scoped
# vector's subset is small relative to a sibling sharing its encoder.
# ---------------------------------------------------------------------------
class TestEmbedDataframeEncoderBatching:
    def test_scoped_vectors_sharing_encoder_and_prompt_name_merge_into_one_call(self):
        # movies_content/books_content-shaped setup: two scoped vectors on
        # the same encoder and prompt_name must be encoded in a single call.
        def encode_from_prompts(prompts, **kwargs):
            return np.array([[float(len(p)), float(ord(p[-1]))] for p in prompts])

        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.side_effect = encode_from_prompts

        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "c", "d"],
                "content_hash": ["h1", "h2", "h3", "h4"],
                "category_id": ["CINEMA", "LIVRE", "CINEMA", "LIVRE"],
                "title": ["Movie A", "Book B", "Movie C", "Book D"],
            }
        )
        movie_vector = Vector(
            name="movies_content",
            features=["title"],
            encoder_name="shared/model",
            prompt_name="document",
            category_filter=_simple_filter("category_id", values=["CINEMA"]),
        )
        book_vector = Vector(
            name="books_content",
            features=["title"],
            encoder_name="shared/model",
            prompt_name="document",
            category_filter=_simple_filter("category_id", values=["LIVRE"]),
        )
        encoders = {"shared/model": mock_encoder}

        result = embed_dataframe(df, [movie_vector, book_vector], encoders)

        assert mock_encoder.encode.call_count == 1
        (called_prompts,), _ = mock_encoder.encode.call_args
        assert sorted(called_prompts) == sorted(
            ["title : Movie A", "title : Movie C", "title : Book B", "title : Book D"]
        )

        # Each item keeps the embedding built from its own prompt, proving
        # the merged result was split back correctly, not just that fewer
        # calls happened.
        by_id = result.set_index("item_id")
        for item_id, prompt, column in [
            ("a", "title : Movie A", "movies_content"),
            ("c", "title : Movie C", "movies_content"),
            ("b", "title : Book B", "books_content"),
            ("d", "title : Book D", "books_content"),
        ]:
            expected = [float(len(prompt)), float(ord(prompt[-1]))]
            assert by_id.loc[item_id, column] == expected

    def test_shared_encoder_different_prompt_name_does_not_merge(self):
        # Merging prompts across different prompt_names would silently
        # corrupt one vector's prefix, so they must stay separate calls.
        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.side_effect = lambda prompts, **kwargs: np.array(
            [[float(len(p)), 0.0] for p in prompts]
        )

        df = pd.DataFrame(
            {
                "item_id": ["a", "b"],
                "content_hash": ["h1", "h2"],
                "category_id": ["CINEMA", "LIVRE"],
                "title": ["Movie A", "Book B"],
            }
        )
        movie_vector = Vector(
            name="movies_content",
            features=["title"],
            encoder_name="shared/model",
            prompt_name="document",
            category_filter=_simple_filter("category_id", values=["CINEMA"]),
        )
        book_vector = Vector(
            name="books_content",
            features=["title"],
            encoder_name="shared/model",
            prompt_name="query",
            category_filter=_simple_filter("category_id", values=["LIVRE"]),
        )
        encoders = {"shared/model": mock_encoder}

        embed_dataframe(df, [movie_vector, book_vector], encoders)

        assert mock_encoder.encode.call_count == 2
        seen_prompt_names = {
            call.kwargs["prompt_name"] for call in mock_encoder.encode.call_args_list
        }
        assert seen_prompt_names == {"document", "query"}
        for call in mock_encoder.encode.call_args_list:
            (prompts,) = call.args
            if call.kwargs["prompt_name"] == "document":
                assert prompts == ["title : Movie A"]
            else:
                assert prompts == ["title : Book B"]

    def test_global_and_scoped_vector_sharing_encoder_merge_into_one_call(self):
        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.side_effect = lambda prompts, **kwargs: np.array(
            [[float(len(p)), 0.0] for p in prompts]
        )

        df = pd.DataFrame(
            {
                "item_id": ["a", "b"],
                "content_hash": ["h1", "h2"],
                "category_id": ["CINEMA", "LIVRE"],
                "name": ["Alice", "Bob"],
                "title": ["Movie A", "Book B"],
            }
        )
        global_vector = Vector(
            name="semantic_content",
            features=["name"],
            encoder_name="shared/model",
            prompt_name="document",
        )
        movie_vector = Vector(
            name="movies_content",
            features=["title"],
            encoder_name="shared/model",
            prompt_name="document",
            category_filter=_simple_filter("category_id", values=["CINEMA"]),
        )
        encoders = {"shared/model": mock_encoder}

        result = embed_dataframe(df, [global_vector, movie_vector], encoders)

        assert mock_encoder.encode.call_count == 1
        (called_prompts,), _ = mock_encoder.encode.call_args
        assert sorted(called_prompts) == sorted(
            ["name : Alice", "name : Bob", "title : Movie A"]
        )

        by_id = result.set_index("item_id")
        assert by_id["semantic_content"].notna().all()
        assert isinstance(by_id.loc["a", "movies_content"], list)
        assert pd.isna(by_id.loc["b", "movies_content"])

    def test_singleton_group_reuses_prompts_list_without_copying(self):
        # Unfiltered/no-sharing configs (e.g. default.yaml's single vector)
        # must do zero extra per-item work: the same prompts list built
        # during preparation is passed straight to encode(), not rebuilt.
        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.return_value = np.array([[1.0, 2.0]])

        vector = Vector(
            name="semantic_content", features=["name"], encoder_name="model"
        )
        prompts = ["name : Alice"]
        entry = _PendingVectorEmbed(vector=vector, prompts=prompts)

        _batch_encode([entry], {"model": mock_encoder}, pools={})

        (called_prompts,), _ = mock_encoder.encode.call_args
        assert called_prompts is prompts


# ---------------------------------------------------------------------------
# embed_dataframe: long-prompt tracking wired end-to-end
# ---------------------------------------------------------------------------
class TestEmbedDataframeLongPromptTracking:
    def _mock_encoder(self, max_seq_length, token_counts):
        encoder = MagicMock()
        encoder.device = "cpu"
        encoder.max_seq_length = max_seq_length
        encoder.prompts = {}
        encoder.tokenizer.return_value = {"input_ids": [[0] * n for n in token_counts]}
        encoder.encode.side_effect = lambda prompts, **kwargs: np.array(
            [[1.0, 2.0]] * len(prompts)
        )
        return encoder

    def test_tracker_records_long_item_but_still_embeds_it(self):
        df = pd.DataFrame(
            {
                "item_id": ["a", "b"],
                "content_hash": ["h1", "h2"],
                "name": ["x" * 50, "short"],
            }
        )
        vector = Vector(name="v", features=["name"], encoder_name="model")
        tracker = LongPromptTracker(max_tokens=20)
        # Only item "a" ("name : " + 50 x's = 57 chars) clears the 2*20=40
        # char pre-filter; item "b" ("name : short" = 12 chars) does not.
        encoder = self._mock_encoder(max_seq_length=20, token_counts=[25])

        result = embed_dataframe(df, [vector], {"model": encoder}, tracker=tracker)

        # Truncation, not exclusion: both items still make it into the output.
        assert sorted(result["item_id"].tolist()) == ["a", "b"]
        assert tracker.long_item_ids == {"v": ["a"]}

    def test_no_tracker_argument_defaults_to_fresh_unreported_tracker(self):
        df = pd.DataFrame({"item_id": ["a"], "content_hash": ["h1"], "name": ["short"]})
        vector = Vector(name="v", features=["name"], encoder_name="model")
        encoder = self._mock_encoder(max_seq_length=2048, token_counts=[])

        result = embed_dataframe(df, [vector], {"model": encoder})

        assert len(result) == 1
