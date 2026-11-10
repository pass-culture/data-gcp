import polars as pl

from src.vocabulary.delta import hash_description, hash_descriptions, select_delta


def test_hash_description_is_deterministic_sha256_hex() -> None:
    digest = hash_description("hello")
    assert digest == hash_description("hello")
    assert len(digest) == 64
    assert all(char in "0123456789abcdef" for char in digest)


def test_hash_description_differs_for_different_text() -> None:
    assert hash_description("a") != hash_description("b")


def test_hash_descriptions_vectorized_matches_scalar() -> None:
    hashed = hash_descriptions(pl.Series(["a", "b"]))
    assert hashed.to_list() == [hash_description("a"), hash_description("b")]


def _documents(item_ids: list[str], hashes: list[str]) -> pl.DataFrame:
    return pl.DataFrame({"item_id": item_ids, "description_hash": hashes})


def test_select_delta_selects_everything_on_first_run() -> None:
    current = _documents(["1", "2"], ["h1", "h2"])
    delta = select_delta(
        current, None, processing_version="v1", previous_processing_version=None
    )
    assert set(delta["item_id"].to_list()) == {"1", "2"}
    assert not delta["is_deleted"].any()


def test_select_delta_skips_unchanged_items() -> None:
    current = _documents(["1", "2"], ["h1", "h2"])
    previous = _documents(["1", "2"], ["h1", "h2"])
    delta = select_delta(
        current, previous, processing_version="v1", previous_processing_version="v1"
    )
    assert delta.filter(~pl.col("is_deleted")).is_empty()


def test_select_delta_selects_changed_item() -> None:
    current = _documents(["1", "2"], ["h1-new", "h2"])
    previous = _documents(["1", "2"], ["h1-old", "h2"])
    delta = select_delta(
        current, previous, processing_version="v1", previous_processing_version="v1"
    )
    new_or_changed = delta.filter(~pl.col("is_deleted"))
    assert new_or_changed["item_id"].to_list() == ["1"]


def test_select_delta_selects_new_item() -> None:
    current = _documents(["1", "2"], ["h1", "h2"])
    previous = _documents(["1"], ["h1"])
    delta = select_delta(
        current, previous, processing_version="v1", previous_processing_version="v1"
    )
    new_or_changed = delta.filter(~pl.col("is_deleted"))
    assert new_or_changed["item_id"].to_list() == ["2"]


def test_select_delta_flags_deleted_item() -> None:
    current = _documents(["1"], ["h1"])
    previous = _documents(["1", "2"], ["h1", "h2"])
    delta = select_delta(
        current, previous, processing_version="v1", previous_processing_version="v1"
    )
    deleted = delta.filter(pl.col("is_deleted"))
    assert deleted["item_id"].to_list() == ["2"]


def test_select_delta_processing_version_bump_reprocesses_everything() -> None:
    current = _documents(["1", "2"], ["h1", "h2"])
    previous = _documents(["1", "2"], ["h1", "h2"])  # unchanged hashes
    delta = select_delta(
        current, previous, processing_version="v2", previous_processing_version="v1"
    )
    new_or_changed = delta.filter(~pl.col("is_deleted"))
    assert set(new_or_changed["item_id"].to_list()) == {"1", "2"}
