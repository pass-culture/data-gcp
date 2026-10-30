import json
import re

import pandas as pd
import pytest

from src.gtl_tree import OWN_ITEMS_NAME, build_tree, check_totals, render_html


def _counts(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "titelive_gtl_id",
            "gtl_label_level_1",
            "gtl_label_level_2",
            "gtl_label_level_3",
            "gtl_label_level_4",
            "n_items",
        ],
    )


@pytest.fixture
def tree():
    return build_tree(
        _counts(
            [
                ("01", "Littérature", "Roman", None, None, 10),
                ("02", "Littérature", "Poésie", None, None, 5),
                ("03", "Littérature", None, None, None, 3),  # labelled at level 1 only
                ("04", "Jeunesse", "Albums", "Tout-petits", None, 7),
                ("05", None, None, None, None, 100),  # no level 1: left out
            ]
        )
    )


def test_totals_cover_whole_subtrees(tree):
    assert tree["name"] == "Livres"
    assert tree["total"] == 25
    by_name = {c["name"]: c for c in tree["children"]}
    assert by_name["Littérature"]["total"] == 18
    assert by_name["Jeunesse"]["total"] == 7
    assert by_name["Jeunesse"]["children"][0]["children"][0]["gtl"] == ["04"]


def test_items_labelled_at_a_parent_become_a_pseudo_child(tree):
    litterature = next(c for c in tree["children"] if c["name"] == "Littérature")
    pseudo = [c for c in litterature["children"] if c.get("pseudo")]
    assert len(pseudo) == 1
    assert pseudo[0]["name"] == OWN_ITEMS_NAME
    assert pseudo[0]["total"] == 3
    assert litterature["paths"] == 3


def test_wrong_totals_are_detected(tree):
    tree["children"][0]["total"] += 1
    with pytest.raises(ValueError, match="node totals are wrong"):
        check_totals(tree, {("Littérature", "Roman"): 10})


def test_page_embeds_the_tree_safely():
    tree = build_tree(_counts([("01", "A </script> B", None, None, None, 1)]))
    page = render_html(tree)
    assert "__DATA__" not in page
    data = re.search(r"const ROOT = (\{.*?\});\n", page).group(1)
    assert "</script>" not in data
    embedded = json.loads(data.replace("<\\/", "</"))
    assert embedded["children"][0]["name"] == "A </script> B"
