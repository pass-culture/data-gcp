from src.vocabulary.lemmatization import lemmatize_documents

SPACY_MODEL = "fr_core_news_sm"
SENTENCE = "Les chats aimaient jouer avec les enfants."


def test_lemmatize_documents_extracts_noun_and_verb_lemmas() -> None:
    [(nouns, verbs)] = lemmatize_documents(
        [SENTENCE], spacy_model=SPACY_MODEL, pos={"NOUN", "VERB"}
    )
    assert "chat" in nouns
    assert "enfant" in nouns
    assert "aimer" in verbs
    assert "jouer" in verbs


def test_lemmatize_documents_filters_by_requested_pos() -> None:
    [(nouns, verbs)] = lemmatize_documents(
        [SENTENCE], spacy_model=SPACY_MODEL, pos={"NOUN"}
    )
    assert nouns
    assert verbs == []


def test_lemmatize_documents_preserves_text_order() -> None:
    texts = [SENTENCE, "Un enfant mangeait une pomme."]
    results = lemmatize_documents(texts, spacy_model=SPACY_MODEL, pos={"NOUN", "VERB"})
    assert len(results) == 2
    assert "chat" in results[0][0]
    assert "enfant" in results[1][0]


def test_lemmatize_documents_empty_input_returns_empty_list() -> None:
    assert lemmatize_documents([], spacy_model=SPACY_MODEL, pos={"NOUN", "VERB"}) == []


def test_lemmatize_documents_parallel_path_matches_sequential() -> None:
    texts = [SENTENCE, SENTENCE]
    sequential = lemmatize_documents(
        texts, spacy_model=SPACY_MODEL, pos={"NOUN", "VERB"}, chunk_texts=500
    )
    parallel = lemmatize_documents(
        texts, spacy_model=SPACY_MODEL, pos={"NOUN", "VERB"}, chunk_texts=1
    )
    assert sequential == parallel
