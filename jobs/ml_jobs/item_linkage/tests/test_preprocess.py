import numpy as np
import pandas as pd
import pytest

from constants import MODEL_TYPE, UNKNOWN_PERFORMER
from preprocess import (
    _digits_to_nullable_int,
    preprocess_catalog,
    preprocess_embeddings,
    preprocess_string,
)


class TestPreprocessString:
    def test_lowercases_and_strips(self):
        assert preprocess_string("  Hello World  ") == "hello world"

    def test_removes_punctuation(self):
        assert preprocess_string("Hello, World!") == "hello world"

    def test_removes_accents(self):
        assert preprocess_string("Café Crème") == "cafe creme"

    def test_removes_apostrophe_and_keeps_digits(self):
        assert preprocess_string("L'Été 2020") == "lete 2020"

    def test_empty_string_returns_none(self):
        assert preprocess_string("") is None

    def test_none_returns_none(self):
        assert preprocess_string(None) is None


class TestPreprocessCatalogEdition:
    """Editions are extracted from names carrying a tome/t/vol/episode token."""

    @pytest.mark.parametrize(
        ("offer_name", "expected_edition"),
        [
            ("Naruto tome 3", 3),
            ("One Piece t3", 3),
            ("Album vol2", 2),
        ],
    )
    def test_extracts_edition_from_keyword(self, offer_name, expected_edition):
        df = pd.DataFrame(
            {
                "offer_name": [offer_name],
                "performer": ["x"],
                "offer_description": ["d"],
            }
        )
        assert preprocess_catalog(df)["edition"].iloc[0] == expected_edition

    @pytest.mark.parametrize("offer_name", ["Livre 03", "Berserk 5", "Simple Book"])
    def test_no_edition_without_keyword_is_na(self, offer_name):
        df = pd.DataFrame(
            {
                "offer_name": [offer_name],
                "performer": ["x"],
                "offer_description": ["d"],
            }
        )
        assert pd.isna(preprocess_catalog(df)["edition"].iloc[0])


class TestDigitsToNullableInt:
    def test_returns_int64_dtype(self):
        assert _digits_to_nullable_int(pd.Series(["3"])).dtype == "Int64"

    def test_normalizes_leading_zeros(self):
        # "03" and "3" must land on the same value so editions compare equal.
        out = _digits_to_nullable_int(pd.Series(["03", "3"]))
        assert out.iloc[0] == out.iloc[1] == 3

    def test_nan_maps_to_na(self):
        assert pd.isna(_digits_to_nullable_int(pd.Series([np.nan])).iloc[0])

    def test_oversized_value_maps_to_na(self):
        # ISBN-like numbers leaking through the regex exceed Int64 range and are nulled.
        oversized = str(2**63)
        assert pd.isna(_digits_to_nullable_int(pd.Series([oversized])).iloc[0])

    def test_max_int64_is_kept(self):
        assert _digits_to_nullable_int(pd.Series([str(2**63 - 1)])).iloc[0] == 2**63 - 1


class TestPreprocessCatalogOeuvre:
    def test_oeuvre_strips_edition_token(self):
        df = pd.DataFrame(
            {
                "offer_name": ["Naruto tome 3"],
                "performer": ["x"],
                "offer_description": ["d"],
            }
        )
        assert preprocess_catalog(df)["oeuvre"].iloc[0].strip() == "naruto"

    def test_oeuvre_strips_trailing_number(self):
        df = pd.DataFrame(
            {
                "offer_name": ["Berserk 5"],
                "performer": ["x"],
                "offer_description": ["d"],
            }
        )
        assert preprocess_catalog(df)["oeuvre"].iloc[0].strip() == "berserk"

    def test_oeuvre_unchanged_without_edition(self):
        df = pd.DataFrame(
            {
                "offer_name": ["Simple Book"],
                "performer": ["x"],
                "offer_description": ["d"],
            }
        )
        assert preprocess_catalog(df)["oeuvre"].iloc[0] == "simple book"


class TestPreprocessCatalogPerformer:
    def test_missing_performer_defaults_to_unknown(self):
        df = pd.DataFrame(
            {
                "offer_name": ["Book"],
                "performer": [None],
                "offer_description": ["d"],
            }
        )
        assert preprocess_catalog(df)["performer"].iloc[0] == UNKNOWN_PERFORMER

    def test_performer_is_preprocessed(self):
        df = pd.DataFrame(
            {
                "offer_name": ["Book"],
                "performer": ["  Éric Zola!  "],
                "offer_description": ["d"],
            }
        )
        assert preprocess_catalog(df)["performer"].iloc[0] == "eric zola"


class TestPreprocessEmbeddings:
    def _n_dim(self):
        return MODEL_TYPE["n_dim"]

    def test_drops_zero_vectors(self):
        n = self._n_dim()
        chunk = pd.DataFrame(
            {
                "item_id": ["a", "b"],
                "embedding": [list(np.ones(n)), list(np.zeros(n))],
            }
        )
        out = preprocess_embeddings(chunk)
        assert out["item_id"].tolist() == ["a"]

    def test_output_is_l2_normalized(self):
        n = self._n_dim()
        chunk = pd.DataFrame(
            {
                "item_id": ["a"],
                "embedding": [list(np.arange(1, n + 1, dtype=float))],
            }
        )
        out = preprocess_embeddings(chunk)
        norm = np.linalg.norm(np.array(out["vector"].tolist()), axis=1)
        assert norm == pytest.approx(1.0)

    def test_embedding_column_is_replaced_by_vector(self):
        n = self._n_dim()
        chunk = pd.DataFrame({"item_id": ["a"], "embedding": [list(np.ones(n))]})
        out = preprocess_embeddings(chunk)
        assert "vector" in out.columns
        assert "embedding" not in out.columns

    def test_wrong_dimension_raises(self):
        chunk = pd.DataFrame(
            {"item_id": ["a"], "embedding": [list(np.ones(self._n_dim() // 2))]}
        )
        with pytest.raises(ValueError, match="dimensions"):
            preprocess_embeddings(chunk)
