import polars as pl

from src.vocabulary.cleaning import (
    build_boilerplate_pattern,
    clean_descriptions,
    find_boilerplate_segments,
)

BOILERPLATE = (
    "Cette édition a été réalisée en partenariat avec la BnF dans le cadre "
    "de son programme de numérisation."
)


def _descriptions_with_boilerplate(n_with: int, n_without: int) -> list[str]:
    with_boilerplate = [f"Roman numéro {i}. {BOILERPLATE}" for i in range(n_with)]
    without_boilerplate = [
        f"Une autre histoire originale numéro {i}." for i in range(n_without)
    ]
    return with_boilerplate + without_boilerplate


def test_find_boilerplate_segments_detects_repeated_segment() -> None:
    descriptions = _descriptions_with_boilerplate(n_with=50, n_without=50)
    segments = find_boilerplate_segments(
        descriptions, min_segment_chars=40, min_share=0.0003, max_texts=200_000, seed=0
    )
    assert BOILERPLATE in segments


def test_find_boilerplate_segments_respects_min_share_threshold() -> None:
    # 2 out of 1000 distinct texts share a segment: below a 0.01 share
    # (threshold = round(0.01 * 1000) = 10), so it should not be flagged.
    descriptions = _descriptions_with_boilerplate(n_with=2, n_without=998)
    segments = find_boilerplate_segments(
        descriptions, min_segment_chars=40, min_share=0.01, max_texts=200_000, seed=0
    )
    assert BOILERPLATE not in segments


def test_find_boilerplate_segments_drops_short_segments() -> None:
    descriptions = ["Court. Court." for _ in range(10)]
    segments = find_boilerplate_segments(
        descriptions, min_segment_chars=40, min_share=0.0003, max_texts=200_000, seed=0
    )
    assert segments == []


def test_find_boilerplate_segments_sorted_longest_first() -> None:
    short = "Segment répété assez souvent pour compter comme un gabarit standard."
    long_ = short + " Et une phrase en plus qui rallonge ce même gabarit standard."
    descriptions = [long_] * 10 + [short] * 10
    segments = find_boilerplate_segments(
        descriptions, min_segment_chars=20, min_share=0.0003, max_texts=200_000, seed=0
    )
    assert segments[0] == long_ or len(segments[0]) >= len(segments[-1])


def test_clean_descriptions_removes_boilerplate_segment() -> None:
    pattern = build_boilerplate_pattern([BOILERPLATE])
    descriptions = pl.Series(["Roman passionnant. " + BOILERPLATE])
    cleaned = clean_descriptions(descriptions, pattern)
    assert BOILERPLATE not in cleaned.item()
    assert "Roman passionnant." in cleaned.item()


def test_clean_descriptions_removes_boilerplate_pasted_inline_after_title() -> None:
    pattern = build_boilerplate_pattern([BOILERPLATE])
    descriptions = pl.Series([f"Mon Titre. {BOILERPLATE} La suite de l'histoire."])
    cleaned = clean_descriptions(descriptions, pattern)
    assert BOILERPLATE not in cleaned.item()
    assert "Mon Titre." in cleaned.item()
    assert "La suite de l'histoire." in cleaned.item()


def test_clean_descriptions_tolerates_whitespace_variants_of_segment() -> None:
    pattern = build_boilerplate_pattern([BOILERPLATE])
    reflowed = BOILERPLATE.replace(" ", "\n")
    cleaned = clean_descriptions(pl.Series([f"Histoire. {reflowed}"]), pattern)
    assert "partenariat" not in cleaned.item()


def test_clean_descriptions_removes_urls() -> None:
    pattern = build_boilerplate_pattern([])
    cleaned = clean_descriptions(
        pl.Series(["Visitez https://example.com/livre pour en savoir plus."]), pattern
    )
    assert "https://" not in cleaned.item()
    assert "Visitez" in cleaned.item()


def test_clean_descriptions_removes_edition_header() -> None:
    pattern = build_boilerplate_pattern([])
    cleaned = clean_descriptions(
        pl.Series(["Date de l'édition originale: 1920/01/01 Un grand classique."]),
        pattern,
    )
    assert "1920" not in cleaned.item()
    assert "Un grand classique." in cleaned.item()


def test_clean_descriptions_leaves_non_matching_text_unchanged() -> None:
    pattern = build_boilerplate_pattern([BOILERPLATE])
    original = "Une histoire totalement originale sans aucun gabarit."
    cleaned = clean_descriptions(pl.Series([original]), pattern)
    assert cleaned.item() == original
