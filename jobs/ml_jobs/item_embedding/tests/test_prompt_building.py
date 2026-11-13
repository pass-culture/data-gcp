"""Unit tests for prompt_building.build_prompts."""

import pandas as pd
import pytest
from src.config import Vector
from src.prompt_building import build_prompts


def _vector(features, **kwargs):
    return Vector(name="test", features=features, encoder_name="model", **kwargs)


class TestDefaultPrompt:
    def test_basic_label_value(self):
        df = pd.DataFrame({"x": ["hello"], "y": ["world"]})
        assert build_prompts(df, _vector(["x", "y"])) == ["x : hello\ny : world"]

    def test_null_feature_skipped(self):
        df = pd.DataFrame({"x": ["hello"], "y": [None]})
        assert build_prompts(df, _vector(["x", "y"])) == ["x : hello"]

    def test_all_null_produces_empty_string(self):
        df = pd.DataFrame({"x": [None], "y": [None]})
        assert build_prompts(df, _vector(["x", "y"])) == [""]

    def test_empty_prompt_stays_in_position(self):
        df = pd.DataFrame({"x": ["hello", None, "world"]})
        assert build_prompts(df, _vector(["x"])) == ["x : hello", "", "x : world"]

    def test_no_double_separators_with_middle_null(self):
        df = pd.DataFrame({"a": ["v1"], "b": [None], "c": ["v3"]})
        assert build_prompts(df, _vector(["a", "b", "c"])) == ["a : v1\nc : v3"]

    def test_labels_override_column_names(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "author_concat": ["Herbert"]})
        vector = _vector(
            ["offer_name", "author_concat"],
            labels={"offer_name": "titre", "author_concat": "auteur"},
        )
        assert build_prompts(df, vector) == ["titre : Dune\nauteur : Herbert"]

    def test_unmapped_feature_falls_back_to_column_name(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "category_id": ["LIVRE"]})
        vector = _vector(["offer_name", "category_id"], labels={"offer_name": "titre"})
        assert build_prompts(df, vector) == ["titre : Dune\ncategory_id : LIVRE"]

    def test_integral_float_renders_without_decimal_point(self):
        # pandas/pyarrow promotes a numeric column to float64 whenever any
        # chunk has a null, so a present value like 2010 arrives as 2010.0.
        df = pd.DataFrame({"year": [2010.0]})
        assert build_prompts(df, _vector(["year"])) == ["year : 2010"]


class TestTemplatePrompt:
    def test_renders_template(self):
        df = pd.DataFrame(
            {"offer_name": ["Dune"], "offer_description": ["A desert planet"]}
        )
        vector = _vector(
            ["offer_name", "offer_description"],
            prompt_template='Title: "{offer_name}". Description: {offer_description}.',
        )
        assert build_prompts(df, vector) == [
            'Title: "Dune". Description: A desert planet.'
        ]

    def test_missing_value_renders_as_empty_not_none(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "offer_description": [None]})
        vector = _vector(
            ["offer_name", "offer_description"],
            prompt_template="{offer_name} - {offer_description}",
        )
        prompts = build_prompts(df, vector)
        assert prompts == ["Dune - "]
        assert "None" not in prompts[0]

    def test_all_null_row_produces_empty_string(self):
        df = pd.DataFrame({"offer_name": [None], "offer_description": [None]})
        vector = _vector(
            ["offer_name", "offer_description"],
            prompt_template="{offer_name} - {offer_description}",
        )
        assert build_prompts(df, vector) == [""]

    def test_template_referencing_undeclared_field_raises(self):
        # The check now runs eagerly when the Vector is constructed (see
        # src/config.py's model_validator), not lazily during build_prompts.
        with pytest.raises(ValueError, match="prompt_template references field"):
            _vector(["offer_name"], prompt_template="{offer_name} by {author}")

    def test_integral_float_renders_without_decimal_point(self):
        # pandas/pyarrow promotes a numeric column to float64 whenever any
        # chunk has a null, so a present value like 2010 arrives as 2010.0.
        df = pd.DataFrame({"offer_name": ["Dune"], "year": [2010.0]})
        vector = _vector(
            ["offer_name", "year"], prompt_template="{offer_name} ({year})"
        )
        assert build_prompts(df, vector) == ["Dune (2010)"]

    def test_non_integral_float_keeps_decimal_point(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "rating": [3.5]})
        vector = _vector(
            ["offer_name", "rating"], prompt_template="{offer_name} ({rating})"
        )
        assert build_prompts(df, vector) == ["Dune (3.5)"]

    def test_multiline_template_joined_with_space(self):
        df = pd.DataFrame(
            {
                "offer_name": ["Dune"],
                "movie_genres": ["SF, Aventure"],
                "offer_description": ["A desert planet"],
            }
        )
        vector = _vector(
            ["offer_name", "movie_genres", "offer_description"],
            prompt_template=(
                'Ce film est intitulé "{offer_name}".\n'
                "Genres: {movie_genres}.\n"
                "Synopsis: {offer_description}."
            ),
        )
        assert build_prompts(df, vector) == [
            'Ce film est intitulé "Dune". Genres: SF, Aventure. '
            "Synopsis: A desert planet."
        ]

    def test_optional_line_dropped_when_its_only_placeholder_is_empty(self):
        df = pd.DataFrame(
            {
                "offer_name": ["Dune"],
                "movie_genres": [None],
                "offer_description": ["A desert planet"],
            }
        )
        vector = _vector(
            ["offer_name", "movie_genres", "offer_description"],
            prompt_template=(
                'Ce film est intitulé "{offer_name}".\n'
                "Genres: {movie_genres}.\n"
                "Synopsis: {offer_description}."
            ),
        )
        assert build_prompts(df, vector) == [
            'Ce film est intitulé "Dune". Synopsis: A desert planet.'
        ]

    def test_line_kept_when_any_placeholder_present(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "author_concat": [None]})
        vector = _vector(
            ["offer_name", "author_concat"],
            prompt_template='Ce livre est intitulé "{offer_name}" par {author_concat}.',
        )
        assert build_prompts(df, vector) == ['Ce livre est intitulé "Dune" par .']

    def test_all_optional_lines_dropped_yields_empty_prompt(self):
        df = pd.DataFrame(
            {"offer_name": [None], "movie_genres": [None], "offer_description": [None]}
        )
        vector = _vector(
            ["offer_name", "movie_genres", "offer_description"],
            prompt_template=(
                'Ce film est intitulé "{offer_name}".\n'
                "Genres: {movie_genres}.\n"
                "Synopsis: {offer_description}."
            ),
        )
        assert build_prompts(df, vector) == [""]
