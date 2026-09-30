import numpy as np

from model.semantic_space import SemanticSpace


def _build_filter(filters):
    # build_filter is a pure method; bypass __init__ (which opens a LanceDB
    # connection) since we only exercise the SQL predicate construction.
    instance = object.__new__(SemanticSpace)
    return SemanticSpace.build_filter(instance, filters)


class TestBuildFilter:
    def test_none_becomes_is_null(self):
        assert _build_filter({"edition": None}) == "(edition IS NULL)"

    def test_nan_becomes_is_null(self):
        assert _build_filter({"edition": float("nan")}) == "(edition IS NULL)"

    def test_integer_is_unquoted(self):
        assert _build_filter({"edition": 3}) == "(edition = 3)"

    def test_boolean_is_lowercased(self):
        assert _build_filter({"flag": True}) == "(flag = true)"

    def test_string_is_quoted(self):
        assert _build_filter({"subcat": "LIVRE"}) == "(subcat = 'LIVRE')"

    def test_single_quote_is_escaped(self):
        assert _build_filter({"name": "l'ete"}) == "(name = 'l''ete')"

    def test_multiple_filters_joined_with_and(self):
        result = _build_filter(
            {"edition": None, "offer_subcategory_id": "LIVRE_PAPIER"}
        )
        assert result == "(edition IS NULL) AND (offer_subcategory_id = 'LIVRE_PAPIER')"

    def test_numpy_nan_becomes_is_null(self):
        assert _build_filter({"edition": np.nan}) == "(edition IS NULL)"
