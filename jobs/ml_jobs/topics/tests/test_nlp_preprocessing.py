import json

import pandas as pd

from src.nlp_preprocessing import preprocess


def test_entities_are_json_and_survive_separators():
    sample = pd.DataFrame(
        {
            "offer_description": [
                "Victor Hugo raconte la vie des misérables à Paris au XIXe siècle.",
                "これは日本語の本です",
            ]
        }
    )
    out = preprocess(sample)
    entities = [json.loads(e) for e in out["entities"]]
    assert "Victor Hugo" in entities[0]["PERSON"]  # French PER kept by default
    assert out["nlp_status"].tolist() == ["ok", "unsupported_lang"]
    assert entities[1] == {}


def test_entities_roundtrip_through_csv(tmp_path):
    sample = pd.DataFrame({"offer_description": ["Simon & Schuster, Inc. à Paris."]})
    path = tmp_path / "out.csv"
    preprocess(sample).to_csv(path, index=False)
    assert isinstance(json.loads(pd.read_csv(path)["entities"][0]), dict)


def test_precomputed_lang_column_is_reused():
    # French text forced through the English pipeline: proves no re-detection
    sample = pd.DataFrame({"offer_description": ["Le chat dort."], "lang": ["en"]})
    out = preprocess(sample)
    assert out["lang"].tolist() == ["en"]
    assert out.columns.tolist().count("lang") == 1


def test_entity_types_allowlist_filters_french_labels():
    sample = pd.DataFrame(
        {"offer_description": ["Victor Hugo a vécu à Paris."], "lang": ["fr"]}
    )
    default = json.loads(preprocess(sample)["entities"][0])
    assert default == {"PERSON": ["Victor Hugo"], "EXTRA": ["Paris"]}
    only_person = json.loads(preprocess(sample, entity_types=["PERSON"])["entities"][0])
    assert only_person == {"PERSON": ["Victor Hugo"]}


def test_labels_are_aliased_before_the_allowlist():
    # fr_core_news_sm tags Victor Hugo PER and Paris LOC, which
    # ENTITY_LABEL_ALIASES maps to PERSON and EXTRA: asking for the raw
    # labels finds nothing.
    sample = pd.DataFrame(
        {"offer_description": ["Victor Hugo a vécu à Paris."], "lang": ["fr"]}
    )
    extra = json.loads(preprocess(sample, entity_types=["EXTRA"])["entities"][0])
    assert extra == {"EXTRA": ["Paris"]}
    raw = json.loads(preprocess(sample, entity_types=["PER", "LOC"])["entities"][0])
    assert raw == {}


def test_languages_without_a_pipeline_are_flagged_not_processed():
    # Only the languages in SPACY_MODELS are processed (French for now).
    sample = pd.DataFrame(
        {"offer_description": ["Charles Dickens lived in London."], "lang": ["en"]}
    )
    out = preprocess(sample)
    assert out["nlp_status"].tolist() == ["unsupported_lang"]
    assert out["entities"].tolist() == ["{}"]
