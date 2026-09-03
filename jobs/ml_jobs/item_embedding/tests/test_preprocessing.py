"""Unit tests for the preprocessing module."""

from preprocessing import (
    PREPROCESSORS,
    clean_description,
    format_book_classification,
    format_movie_genres,
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


class TestFormatMovieGenres:
    def test_native_envelope(self):
        value = {"movies": {"genres": ["DRAMA", "ACTION"]}}
        assert format_movie_genres(value) == "DRAMA, ACTION"

    def test_json_string_envelope(self):
        value = '{"movies": {"genres": ["DRAMA", "ACTION"]}}'
        assert format_movie_genres(value) == "DRAMA, ACTION"

    def test_single_genre(self):
        assert format_movie_genres({"movies": {"genres": ["DRAMA"]}}) == "DRAMA"

    def test_empty_genres_list_returns_none(self):
        assert format_movie_genres({"movies": {"genres": []}}) is None

    def test_missing_movies_key_returns_none(self):
        # e.g. a books-only envelope reaching the movies preprocessor by
        # mistake -- must not raise, just report no metadata.
        assert format_movie_genres({"books": {"gtl1": "roman"}}) is None

    def test_missing_genres_key_returns_none(self):
        assert format_movie_genres({"movies": {}}) is None

    def test_none_passthrough(self):
        assert format_movie_genres(None) is None

    def test_key_is_fixed_not_tied_to_a_vector_name(self):
        # The envelope key is "movies" regardless of what the config calls
        # the movies-scoped vector (e.g. "movies_content", "films_content",
        # ...) -- renaming the vector must not affect extraction.
        value = {"movies": {"genres": ["DRAMA"]}}
        assert format_movie_genres(value) == "DRAMA"


class TestFormatBookClassification:
    def test_full_hierarchy(self):
        value = {
            "books": {
                "gtl1": "roman",
                "gtl2": "19eme siecle",
                "gtl3": "tragedie",
                "gtl4": None,
            }
        }
        assert format_book_classification(value) == (
            "niveau 1 : roman > niveau 2 : 19eme siecle > niveau 3 : tragedie"
        )

    def test_json_string_envelope(self):
        value = (
            '{"books": {"gtl1": "roman", "gtl2": "19eme siecle", '
            '"gtl3": null, "gtl4": null}}'
        )
        assert (
            format_book_classification(value)
            == "niveau 1 : roman > niveau 2 : 19eme siecle"
        )

    def test_gap_in_the_middle_is_skipped(self):
        # gtl2 missing but gtl3 present: only populated levels are included,
        # but each keeps its own level label, so "tragedie" here is tagged
        # "niveau 3" (its real gtl3 rank), not shifted to "niveau 2".
        value = {"books": {"gtl1": "roman", "gtl2": None, "gtl3": "tragedie"}}
        assert (
            format_book_classification(value)
            == "niveau 1 : roman > niveau 3 : tragedie"
        )

    def test_same_term_at_different_depth_is_labeled_differently(self):
        # "tragedie" at niveau 2 (theatre > tragedie > grecque) must not
        # collapse into the same shape as "tragedie" at niveau 3 (roman >
        # 19eme siecle > tragedie) — the label disambiguates the depth. GTL
        # has no fixed per-level semantic (it isn't always genre > sub-genre
        # > type), so levels are tagged by raw position, not an invented
        # category name.
        roman = {"books": {"gtl1": "roman", "gtl2": "19eme siecle", "gtl3": "tragedie"}}
        theatre = {"books": {"gtl1": "theatre", "gtl2": "tragedie", "gtl3": "grecque"}}
        assert format_book_classification(roman) == (
            "niveau 1 : roman > niveau 2 : 19eme siecle > niveau 3 : tragedie"
        )
        assert format_book_classification(theatre) == (
            "niveau 1 : theatre > niveau 2 : tragedie > niveau 3 : grecque"
        )

    def test_all_levels_missing_returns_none(self):
        value = {"books": {"gtl1": None, "gtl2": None, "gtl3": None, "gtl4": None}}
        assert format_book_classification(value) is None

    def test_missing_books_key_returns_none(self):
        assert format_book_classification({"movies": {"genres": ["DRAMA"]}}) is None

    def test_missing_keys_treated_as_absent(self):
        assert format_book_classification({"books": {}}) is None

    def test_none_passthrough(self):
        assert format_book_classification(None) is None

    def test_key_is_fixed_not_tied_to_a_vector_name(self):
        value = {"books": {"gtl1": "roman"}}
        assert format_book_classification(value) == "niveau 1 : roman"


class TestPreprocessorsRegistry:
    def test_contains_normalize_whitespace(self):
        assert "normalize_whitespace" in PREPROCESSORS
        assert PREPROCESSORS["normalize_whitespace"] is normalize_whitespace

    def test_contains_clean_description(self):
        assert "clean_description" in PREPROCESSORS
        assert PREPROCESSORS["clean_description"] is clean_description

    def test_contains_format_movie_genres(self):
        assert "format_movie_genres" in PREPROCESSORS
        assert PREPROCESSORS["format_movie_genres"] is format_movie_genres

    def test_contains_format_book_classification(self):
        assert "format_book_classification" in PREPROCESSORS
        assert PREPROCESSORS["format_book_classification"] is format_book_classification
