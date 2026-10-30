from src import language
from src.language import detect_languages


def test_detects_french_english_german():
    docs = [
        "Un roman haletant sur la mer et les marins bretons.",
        "A gripping novel about the sea and the sailors.",
        "Ein spannender Roman über das Meer und die Seeleute.",
    ]
    assert detect_languages(docs, n_jobs=1) == ["fr", "en", "de"]


def test_parallel_detection_matches_sequential(monkeypatch):
    docs = ["Un roman sur la mer.", "A novel about the sea.", "Ein Roman."] * 20
    monkeypatch.setattr(language, "_PARALLEL_MIN_DOCS", 1)
    assert detect_languages(docs, n_jobs=2) == detect_languages(docs, n_jobs=1)
