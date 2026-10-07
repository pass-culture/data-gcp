"""Unit tests for the preprocessing module."""

import numpy as np
import pandas as pd
from src.preprocessing import (
    PREPROCESSORS,
    _is_missing,
    apply_preprocessors,
    clean_description,
    normalize_whitespace,
)


class TestNormalizeWhitespace:
    def test_collapses_internal_whitespace(self):
        assert normalize_whitespace("hello   world\t\tfoo") == "hello world foo"

    def test_strips_ends(self):
        assert normalize_whitespace("  hello world  ") == "hello world"

    def test_collapses_newlines(self):
        assert normalize_whitespace("hello\n\nworld") == "hello world"

    def test_none_passthrough(self):
        assert normalize_whitespace(None) is None

    def test_empty_string(self):
        assert normalize_whitespace("") == ""

    def test_already_normalized_is_unchanged(self):
        assert normalize_whitespace("hello world") == "hello world"


class TestCleanDescription:
    def test_strips_http_url(self):
        assert (
            clean_description("Regarde ici http://example.com/movie plus d'infos")
            == "Regarde ici plus d'infos"
        )

    def test_strips_https_url(self):
        assert (
            clean_description("Voir https://example.com/x?y=1 pour le trailer")
            == "Voir pour le trailer"
        )

    def test_strips_www_url(self):
        assert clean_description("Site: www.example.com fin") == "Site: fin"

    def test_strips_allocine_boilerplate(self):
        text = "Un film culte. Tous les détails du film sur AlloCiné: bla bla"
        assert clean_description(text) == "Un film culte. bla bla"

    def test_strips_pour_plus_dinformations_without_apostrophe(self):
        text = "Un film culte. Pour plus d informations, rendez-vous sur bla bla"
        assert clean_description(text) == "Un film culte. bla bla"

    def test_strips_pour_plus_dinformations_with_apostrophe(self):
        text = "Un film culte. Pour plus d'informations, rendez-vous sur bla bla"
        assert clean_description(text) == "Un film culte. bla bla"

    def test_flattens_newlines_to_single_space(self):
        # clean_description delegates whitespace handling to
        # normalize_whitespace, which collapses ALL whitespace (including
        # line breaks) to single spaces, unlike a line-break-preserving
        # cleanup.
        assert clean_description("Ligne 1\n\n\nLigne 2\r\n\r\nLigne 3") == (
            "Ligne 1 Ligne 2 Ligne 3"
        )

    def test_collapses_repeated_spaces_and_tabs_to_single_space(self):
        assert clean_description("mot1   mot2\t\tmot3") == "mot1 mot2 mot3"

    def test_trims_ends(self):
        assert clean_description("   texte propre   ") == "texte propre"

    def test_combined_cleaning(self):
        text = (
            "  Un film de SF.\r\n\r\nTous les détails du film sur AlloCiné: "
            "https://allocine.fr/movie/123   \n\n  www.example.com  \t fin.  "
        )
        assert clean_description(text) == "Un film de SF. fin."

    def test_none_passthrough(self):
        assert clean_description(None) is None

    def test_empty_string(self):
        assert clean_description("") == ""


class TestPreprocessorsRegistry:
    def test_contains_normalize_whitespace(self):
        assert "normalize_whitespace" in PREPROCESSORS
        assert PREPROCESSORS["normalize_whitespace"] is normalize_whitespace

    def test_contains_clean_description(self):
        assert "clean_description" in PREPROCESSORS
        assert PREPROCESSORS["clean_description"] is clean_description


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
        # pd.notna on a list raises when used as a bool; _is_missing must not.
        assert _is_missing(["A", "B"]) is False

    def test_dict_is_not_missing(self):
        assert _is_missing({"k": "v"}) is False


class TestApplyPreprocessors:
    def test_applies_to_configured_columns_only(self):
        df = pd.DataFrame(
            {"offer_name": ["  Dune   Messiah "], "other": ["  keep  as is "]}
        )
        out = apply_preprocessors(df, {"offer_name": "normalize_whitespace"})
        assert out["offer_name"].tolist() == ["Dune Messiah"]
        assert out["other"].tolist() == ["  keep  as is "]

    def test_missing_values_are_not_passed_to_the_function(self):
        df = pd.DataFrame({"offer_description": ["http://x.com hi", None, np.nan]})
        out = apply_preprocessors(df, {"offer_description": "clean_description"})
        assert out["offer_description"].tolist()[0] == "hi"
        assert out["offer_description"].isna().tolist() == [False, True, True]

    def test_empty_mapping_is_a_noop_copy(self):
        df = pd.DataFrame({"a": ["x"]})
        out = apply_preprocessors(df, {})
        assert out.equals(df)
        assert out is not df
