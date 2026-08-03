import pandas as pd

from domain.taxonomy import _parse_location_ids, build_taxonomy

CONFIG = {
    "in_scope_root_collection_ids": [608],
    "certified_ancestor_collection_id": 617,
    "squads": [
        {"collection_id": 617, "label": "global"},
        {"collection_id": 622, "label": "produit"},
        {"collection_id": 621, "label": "tech"},
    ],
    "tier_rules": [
        {"tier": "key_dashboard", "ancestor_title_pattern": "%tableau de bord clé%"},
        {"tier": "key_dashboard", "ancestor_title_pattern": "%indicateurs clés%"},
        {"tier": "thematique", "ancestor_title_pattern": "%thématique%"},
        {"tier": "chantier", "ancestor_title_pattern": "%chantier%"},
    ],
}


def _collections():
    # collection_id, collection_name, location, archived, personal_owner_id
    rows = [
        (608, "1. Interne", "/", False, None),
        (622, "Produit", "/608/", False, None),
        (617, "# Chiffres clés à l'échelle du pass", "/608/", False, None),
        (700, "Chantiers du moment", "/608/622/", False, None),
        (701, "Acquisition Q3", "/608/622/700/", False, None),
        (710, "Tableau de bord clé", "/608/622/", False, None),
        (711, "Indicateurs clés", "/608/621/", False, None),
        (6171, "KPI macro", "/608/617/", False, None),
        (610, "5. Archive", "/", False, None),
        (900, "Vieux truc", "/610/", False, None),
        (950, "Archived card folder", "/608/622/", True, None),
        (999, "Someone's perso", "/", False, 42),
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "collection_id",
            "collection_name",
            "location",
            "archived",
            "personal_owner_id",
        ],
    )


def _by_id(df):
    return {int(r.collection_id): r for r in df.itertuples(index=False)}


class TestParseLocationIds:
    def test_root(self):
        assert _parse_location_ids("/") == []

    def test_nested(self):
        assert _parse_location_ids("/608/622/700/") == [608, 622, 700]

    def test_non_string(self):
        assert _parse_location_ids(None) == []


class TestBuildTaxonomy:
    def setup_method(self):
        self.tax = _by_id(build_taxonomy(_collections(), CONFIG))

    def test_squad_resolved_from_ancestor(self):
        # 701 sits under /608/622/700/ → squad produit (depth-1 collection 622)
        assert self.tax[701].squad == "produit"

    def test_tier_deepest_ancestor_wins(self):
        # 701 is under a "Chantiers du moment" ancestor → chantier
        assert self.tax[701].tier == "chantier"
        # 710 is itself a "Tableau de bord clé" → key_dashboard
        assert self.tax[710].tier == "key_dashboard"

    def test_key_dashboard_matches_indicateurs_cles(self):
        # 711 "Indicateurs clés" → key_dashboard (second pattern)
        assert self.tax[711].tier == "key_dashboard"
        assert self.tax[711].squad == "tech"

    def test_certified_when_617_in_chain(self):
        assert self.tax[6171].certified is True
        assert self.tax[701].certified is False

    def test_global_squad_under_617(self):
        # Collections under "# Chiffres clés à l'échelle du pass" → squad global,
        # and still certified.
        assert self.tax[6171].squad == "global"
        assert self.tax[6171].certified is True

    def test_in_scope_only_under_608(self):
        assert self.tax[701].in_scope is True
        assert self.tax[900].in_scope is False  # under archive root 610

    def test_excluded_only_for_archived_or_personal(self):
        # Out-of-scope-by-location is not "excluded"; the whitelist handles it.
        assert self.tax[900].is_excluded is False  # under archive root, not flagged
        assert self.tax[950].is_excluded is True  # archived flag
        assert self.tax[999].is_excluded is True  # personal collection

    def test_personal_and_archived_not_in_scope(self):
        assert self.tax[950].in_scope is False
        assert self.tax[999].in_scope is False

    def test_no_tier_when_no_matching_ancestor(self):
        assert self.tax[622].tier is None
        assert self.tax[622].squad == "produit"  # squad itself
