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
from embedding import (
    LongPromptTracker,
    _build_prompts,
    _category_filter_mask,
    _find_long_prompts,
    _is_missing,
    embed_dataframe,
)


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
        from constants import MAX_PROMPT_TOKENS

        assert LongPromptTracker().max_tokens == MAX_PROMPT_TOKENS

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
