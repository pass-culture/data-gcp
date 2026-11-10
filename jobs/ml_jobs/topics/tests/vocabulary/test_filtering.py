import polars as pl
import pytest

from src.vocabulary import filtering


def _items(**overrides: list[object]) -> pl.DataFrame:
    base = {
        "item_id": ["1", "2", "3", "4", "5"],
        "gtl_type": ["BOOK", "BOOK", "BOOK", "MOVIE", "BOOK"],
        "gtl_id": ["01000000", None, "02000000", "03000000", "04000000"],
        "gtl_label_level_1": ["Littérature", "Jeunesse", "", "BD", "Arts"],
        "offer_description": [
            "  Une très belle histoire.  ",
            "Un roman pour enfants.",
            "Un livre sans genre.",
            "Une bande dessinée.",
            "   ",
        ],
    }
    base.update(overrides)
    return pl.DataFrame(base)


def test_filter_book_documents_drops_non_book_rows() -> None:
    result = filtering.filter_book_documents(_items())
    assert "4" not in result["item_id"].to_list()


def test_filter_book_documents_drops_null_gtl_id() -> None:
    result = filtering.filter_book_documents(_items())
    assert "2" not in result["item_id"].to_list()


def test_filter_book_documents_drops_empty_gtl_label_level_1() -> None:
    result = filtering.filter_book_documents(_items())
    assert "3" not in result["item_id"].to_list()


def test_filter_book_documents_drops_empty_description_after_trim() -> None:
    result = filtering.filter_book_documents(_items())
    assert "5" not in result["item_id"].to_list()


def test_filter_book_documents_trims_description() -> None:
    result = filtering.filter_book_documents(_items())
    kept = result.filter(pl.col("item_id") == "1")["offer_description"].item()
    assert kept == "Une très belle histoire."


def test_filter_book_documents_deduplicates_on_item_id() -> None:
    items = pl.DataFrame(
        {
            "item_id": ["1", "1"],
            "gtl_type": ["BOOK", "BOOK"],
            "gtl_id": ["01000000", "01000000"],
            "gtl_label_level_1": ["Littérature", "Littérature"],
            "offer_description": ["Premier.", "Deuxième."],
        }
    )
    result = filtering.filter_book_documents(items)
    assert result.height == 1


@pytest.mark.parametrize("threshold", [50_000, 1])
def test_filter_french_keeps_only_french_descriptions(
    monkeypatch: pytest.MonkeyPatch, threshold: int
) -> None:
    """Runs once below the parallel threshold and once forced above it, so
    both code paths are exercised against real py3langid.
    """
    monkeypatch.setattr(filtering, "LANGUAGE_DETECTION_PARALLEL_THRESHOLD", threshold)
    items = pl.DataFrame(
        {
            "item_id": ["1", "2"],
            "offer_description": [
                "Ceci est une magnifique histoire française pleine de rebondissements.",
                "This is a wonderful English novel full of adventure and surprise.",
            ],
        }
    )
    result = filtering.filter_french(items)
    assert result["item_id"].to_list() == ["1"]


def test_filter_documents_combines_both_steps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(filtering, "LANGUAGE_DETECTION_PARALLEL_THRESHOLD", 50_000)
    items = _items()
    items = items.with_columns(
        pl.when(pl.col("item_id") == "1")
        .then(pl.lit("Une magnifique histoire française pleine de rebondissements."))
        .otherwise(pl.col("offer_description"))
        .alias("offer_description")
    )
    result = filtering.filter_documents(items)
    assert result["item_id"].to_list() == ["1"]
