import pandas as pd

from src.preprocessing import (
    description_preprocessing,
    full_name_preprocessing,
    offer_name_preprocessing,
)


class TestDescriptionPreprocessing:
    def test_lowercases_and_strips(self):
        series = pd.Series(["  Hello World this is a long description  "])
        result = description_preprocessing(series, min_length=30)
        assert result.iloc[0] == "hello world this is a long description"

    def test_removes_accents(self):
        series = pd.Series(["Événement spécial avec des artistes célèbres"])
        result = description_preprocessing(series, min_length=30)
        assert result.iloc[0] == "evenement special avec des artistes celebres"

    def test_short_descriptions_become_na(self):
        series = pd.Series(["Too short"])
        result = description_preprocessing(series, min_length=30)
        assert pd.isna(result.iloc[0])

    def test_exactly_min_length_is_kept(self):
        series = pd.Series(["a" * 30])
        result = description_preprocessing(series, min_length=30)
        assert result.iloc[0] == "a" * 30

    def test_below_min_length_is_na(self):
        series = pd.Series(["a" * 29])
        result = description_preprocessing(series, min_length=30)
        assert pd.isna(result.iloc[0])

    def test_multiple_values(self):
        series = pd.Series(
            [
                "This is a valid long description text!",
                "Short",
                "Another valid description that is long enough",
            ]
        )
        result = description_preprocessing(series, min_length=30)
        assert result.iloc[0] == "this is a valid long description text!"
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == "another valid description that is long enough"


class TestOfferNamePreprocessing:
    def test_lowercases_and_strips(self):
        series = pd.Series(["  Artist Name  "])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist name"

    def test_removes_concert_prefix(self):
        series = pd.Series(["Concert Artist Name"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist name"

    def test_removes_concerts_prefix(self):
        series = pd.Series(["Concerts Artist Name"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist name"

    def test_removes_concert_de_poche_prefix(self):
        series = pd.Series(["Concert de poche Artist"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist"

    def test_removes_apero_concert_prefix(self):
        series = pd.Series(["Apero-concert Artist"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist"

    def test_splits_on_slash(self):
        series = pd.Series(["Artist A / Artist B"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist a"

    def test_splits_on_plus(self):
        series = pd.Series(["Artist A + Artist B"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist a"

    def test_splits_on_ampersand(self):
        series = pd.Series(["Artist A & Artist B"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist a"

    def test_splits_on_dash(self):
        series = pd.Series(["Artist A - Artist B"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist a"

    def test_splits_on_pipe(self):
        series = pd.Series(["Artist A | Artist B"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "artist a"

    def test_removes_accents(self):
        series = pd.Series(["Élodie François"])
        result = offer_name_preprocessing(series)
        assert result.iloc[0] == "elodie francois"

    def test_empty_after_processing_becomes_na(self):
        series = pd.Series(["Concert"])
        result = offer_name_preprocessing(series)
        assert pd.isna(result.iloc[0])


class TestFullNamePreprocessing:
    def test_lowercases_and_strips(self):
        series = pd.Series(["  Concert Name  "])
        result = full_name_preprocessing(series)
        assert result.iloc[0] == "concert name"

    def test_removes_accents(self):
        series = pd.Series(["Événement Spécial"])
        result = full_name_preprocessing(series)
        assert result.iloc[0] == "evenement special"

    def test_empty_string_becomes_na(self):
        series = pd.Series([""])
        result = full_name_preprocessing(series)
        assert pd.isna(result.iloc[0])

    def test_preserves_non_empty_content(self):
        series = pd.Series(["simple name"])
        result = full_name_preprocessing(series)
        assert result.iloc[0] == "simple name"
