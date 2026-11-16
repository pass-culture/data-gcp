import pandas as pd
import pytest

from src.linkage.constants import Action, Comment
from src.linkage.wiki_link_preservation import (
    are_artist_names_compatible,
    preserve_wiki_links_replaced_by_compatible_artists,
)


@pytest.mark.parametrize(
    ("name_a", "name_b"),
    [
        ("Olivier Kempf", "Kempf"),
        ("Erik Satie", "Satie"),
        ("Thierry Lefebvre", "Lefebvre"),
        ("Bénédicte Zimmermann", "Zimmermann"),
        ("Pierre Kalmar", "P. Kalmar"),
        ("Roger E. Moore", "Moore Roger"),
        ("Igor Stravinsky", "Igor"),
        ("Dermot Bolger", "Dermot"),
        ("Jean-François Kahn", "J. F. Kahn"),
        ("Paul Landowski", "Paul Landovski"),
        ("Castil-Blaze", "Alfred De Castil Blaze"),
        ("Fiodor Dostoïevski", "Dostoievski"),
    ],
)
def test_compatible_names(name_a, name_b):
    assert are_artist_names_compatible(name_a, name_b)
    assert are_artist_names_compatible(name_b, name_a)


@pytest.mark.parametrize(
    ("name_a", "name_b"),
    [
        ("Delphine Horvilleur", "Michel Horvilleur"),
        ("Emily Brontë", "Charlotte Brontë"),
        ("Robert Louis Stevenson", "Neal Stevenson"),
        ("Jean-Claude Fournier", "Cécile Fournier"),
        ("Katherine Arden", "Katherine Webber"),
        ("Léopold Sédar Senghor", "Leopold Von"),
        ("Pierre Kalmar", "P. K."),
        ("Collectif", "Collectif"),
        ("Erik Satie", None),
    ],
)
def test_incompatible_names(name_a, name_b):
    assert not are_artist_names_compatible(name_a, name_b)
    assert not are_artist_names_compatible(name_b, name_a)


def _artist(artist_id, artist_name, wikidata_id=None):
    return {
        "artist_id": artist_id,
        "artist_name": artist_name,
        "wikidata_id": wikidata_id,
    }


def _link(product_id, artist_id, action, comment, artist_type="author"):
    return {
        "offer_product_id": product_id,
        "artist_id": artist_id,
        "artist_type": artist_type,
        "action": action,
        "comment": comment,
    }


def _remove(product_id, artist_id, artist_type="author"):
    return _link(
        product_id, artist_id, Action.remove, Comment.removed_linked, artist_type
    )


def _add_new(product_id, artist_id, artist_type="author"):
    return _link(
        product_id, artist_id, Action.add, Comment.linked_to_new_artist, artist_type
    )


def _add_existing(product_id, artist_id, artist_type="author"):
    return _link(
        product_id,
        artist_id,
        Action.add,
        Comment.linked_to_existing_artist,
        artist_type,
    )


@pytest.fixture()
def artist_df():
    return pd.DataFrame(
        [
            _artist("kempf_wiki", "Olivier Kempf", "Q1"),
            _artist("bronte_wiki", "Emily Brontë", "Q2"),
            _artist("dosto_wiki", "Fiodor Dostoïevski", "Q3"),
            _artist("kalmar", "P. Kalmar"),
            _artist("charlotte_wiki", "Charlotte Brontë", "Q4"),
        ]
    )


def _run(delta_product_rows, delta_artist_rows, artist_df, wiki_df=None):
    delta_artist_df = pd.DataFrame(
        delta_artist_rows, columns=["artist_id", "artist_name", "wikidata_id"]
    )
    return preserve_wiki_links_replaced_by_compatible_artists(
        delta_product_df=pd.DataFrame(delta_product_rows),
        delta_artist_df=delta_artist_df,
        artist_df=artist_df,
        wiki_df=wiki_df,
    )


def _links(delta_product_df):
    return set(
        delta_product_df.loc[:, ["offer_product_id", "artist_id", "action"]].itertuples(
            index=False, name=None
        )
    )


def test_compatible_new_artist_swap_is_cancelled(artist_df):
    delta_product_df, delta_artist_df = _run(
        [_remove(1, "kempf_wiki"), _add_new(1, "kempf_new")],
        [_artist("kempf_new", "Kempf")],
        artist_df,
    )

    assert delta_product_df.empty
    assert delta_artist_df.empty


def test_compatible_existing_artist_swap_is_cancelled(artist_df):
    artist_df = pd.concat(
        [artist_df, pd.DataFrame([_artist("kalmar_wiki", "Pierre Kalmar", "Q5")])],
        ignore_index=True,
    )

    delta_product_df, _ = _run(
        [_remove(1, "kalmar_wiki"), _add_existing(1, "kalmar")], [], artist_df
    )

    assert delta_product_df.empty


def test_split_name_creates_no_parasite_artists(artist_df):
    """'Kempf, Olivier' split in two rows must not create 'Kempf' and 'Olivier'."""
    delta_product_df, delta_artist_df = _run(
        [
            _remove(1, "kempf_wiki"),
            _add_new(1, "kempf_new"),
            _add_new(1, "olivier_new"),
        ],
        [_artist("kempf_new", "Kempf"), _artist("olivier_new", "Olivier")],
        artist_df,
    )

    assert delta_product_df.empty
    assert delta_artist_df.empty


def test_wikidata_alias_is_used(artist_df):
    wiki_df = pd.DataFrame(
        {"wikidata_id": ["Q3"], "raw_alias": ["Fedor Mikhaïlovitch Dostoïevski"]}
    )

    delta_product_df, _ = _run(
        [_remove(1, "dosto_wiki"), _add_new(1, "dosto_new")],
        [_artist("dosto_new", "Mikhailovitch Dostoievski")],
        artist_df,
        wiki_df,
    )

    assert delta_product_df.empty


def test_incompatible_swap_is_kept(artist_df):
    delta_product_df, delta_artist_df = _run(
        [_remove(1, "bronte_wiki"), _add_new(1, "anne_new")],
        [_artist("anne_new", "Anne Brontë")],
        artist_df,
    )

    assert _links(delta_product_df) == {
        (1, "bronte_wiki", Action.remove),
        (1, "anne_new", Action.add),
    }
    assert list(delta_artist_df.artist_id) == ["anne_new"]


def test_replacement_by_wiki_artist_is_kept(artist_df):
    delta_product_df, _ = _run(
        [_remove(1, "bronte_wiki"), _add_existing(1, "charlotte_wiki")],
        [],
        artist_df,
    )

    assert len(delta_product_df) == 2


def test_removed_non_wiki_artist_is_not_preserved(artist_df):
    delta_product_df, _ = _run(
        [_remove(1, "kalmar"), _add_new(1, "kalmar_new")],
        [_artist("kalmar_new", "Kalmar")],
        artist_df,
    )

    assert len(delta_product_df) == 2


def test_other_products_and_artist_types_are_untouched(artist_df):
    delta_product_df, delta_artist_df = _run(
        [
            _remove(1, "kempf_wiki"),
            _add_new(2, "kempf_new"),
            _add_new(1, "kempf_new", artist_type="performer"),
        ],
        [_artist("kempf_new", "Kempf")],
        artist_df,
    )

    assert len(delta_product_df) == 3
    assert list(delta_artist_df.artist_id) == ["kempf_new"]


def test_new_artist_still_linked_elsewhere_is_kept(artist_df):
    delta_product_df, delta_artist_df = _run(
        [
            _remove(1, "kempf_wiki"),
            _add_new(1, "kempf_new"),
            _add_new(2, "kempf_new"),
        ],
        [_artist("kempf_new", "Kempf")],
        artist_df,
    )

    assert _links(delta_product_df) == {(2, "kempf_new", Action.add)}
    assert list(delta_artist_df.artist_id) == ["kempf_new"]
