import re

import pandas as pd
import polars as pl

from src.text_cleaning import boilerplate_regex, remove_boilerplate, strip_all

TEMPLATE = [
    "Le présent ouvrage s'inscrit dans une politique de conservation patrimoniale.",
    "Pour plus d'informations, rendez-vous sur le site de l'éditeur partenaire.",
]


def _corpus() -> pd.DataFrame:
    texts = []
    for i in range(1_000):
        title = f"Titre ancien numéro {i} réimprimé par la bibliothèque nationale"
        if i % 2:  # one sentence per line
            texts.append("\n".join([title, *TEMPLATE]))
        else:  # pasted inline, no sentence break after the title
            texts.append(f"{title} {' '.join(TEMPLATE)}")
    texts += [
        f"Un vrai résumé unique de roman, avec une intrigue {i}." for i in range(1_000)
    ]
    return pd.DataFrame({"offer_description": texts})


def test_template_removed_inline_and_per_line():
    out = remove_boilerplate(_corpus(), min_chars=0)
    joined = " ".join(out["offer_description"])
    assert "conservation patrimoniale" not in joined
    assert "rendez-vous" not in joined
    assert out["offer_description"][0].startswith("Titre ancien numéro 0")
    assert out["offer_description_raw"][0] == _corpus()["offer_description"][0]


def test_unique_descriptions_untouched_and_short_leftovers_dropped():
    df = _corpus()
    df.loc[len(df)] = "Titre court " + " ".join(TEMPLATE)
    out = remove_boilerplate(df, min_chars=50)
    real = out[out["offer_description"].str.startswith("Un vrai résumé")]
    assert len(real) == 1_000
    assert (real["offer_description"] == real["offer_description_raw"]).all()
    assert not out["offer_description"].str.startswith("Titre court").any()


def test_urls_and_original_edition_date_are_stripped():
    df = pd.DataFrame(
        {
            "offer_description": [
                "Histoire de France, tome 3. Date de l'édition originale : 1876 "
                "Voir http://gallica.bnf.fr/ark:/12148/bpt6k5773032h et www.x.fr"
            ]
        }
    )
    text = remove_boilerplate(df, min_chars=0)["offer_description"][0]
    assert text == "Histoire de France, tome 3. Voir et"


def _python_reference(text: str, regex: str) -> str:
    cleaned, n = re.subn(regex, " ", text)
    if n == 0:
        return text
    return re.sub(r"[ \t]+", " ", re.sub(r"\s*\n\s*", "\n", cleaned)).strip()


def test_polars_strip_matches_python_re_whatever_the_slicing():
    texts = [
        *_corpus()["offer_description"],
        "  Pour PLUS d'informations, rendez-vous "
        "sur le site de l'éditeur partenaire.  ",
        None,
    ]
    regex = boilerplate_regex(TEMPLATE)
    series = pl.Series("t", texts)
    one = strip_all(series, regex, n_slices=1)
    assert strip_all(series, regex, n_slices=7).to_list() == one.to_list()
    expected = [None if t is None else _python_reference(t, regex) for t in texts]
    assert one.to_list() == expected
    assert one[-2] == ""  # case-insensitive match
