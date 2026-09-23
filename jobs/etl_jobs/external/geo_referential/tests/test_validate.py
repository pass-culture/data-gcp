import os

import pandas as pd
import pytest

os.environ.setdefault("GCP_PROJECT_ID", "test-project")

from main import validate  # noqa: E402
from utils.sources import Extract  # noqa: E402


def _extracts(**overrides: pd.DataFrame) -> dict[str, Extract]:
    frames = {
        "ign_contour_iris": pd.DataFrame(
            {
                "iris_code": ["593500101", "975020000"],
                "city_code": ["59350", "97502"],
                "geometry_wkt": ["POINT (0 0)", "POINT (1 1)"],
            }
        ),
        "geo_api_gouv_commune_contour": pd.DataFrame(
            {"city_code": ["98612"], "geometry_wkt": ["POINT (2 2)"]}
        ),
        "insee_cog_commune": pd.DataFrame(
            {"typecom": ["COM", "ARM"], "com": ["59350", "75101"]}
        ),
        "insee_cog_commune_comer": pd.DataFrame(
            {
                "com_comer": ["97502", "98612", "98412"],
                "nature_zonage": ["COM", "CIR", "DIS"],
            }
        ),
        "insee_epci": pd.DataFrame({"epci_code": ["200093201"]}),
        "insee_epci_commune": pd.DataFrame(
            {"city_code": ["59350"], "epci_code": ["200093201"]}
        ),
    }
    frames.update(overrides)
    return {name: Extract(name, 2026, df) for name, df in frames.items()}


def test_validate_passes_on_consistent_extracts():
    validate(_extracts())


def test_validate_fails_when_an_inhabited_collectivity_commune_has_no_geometry():
    extracts = _extracts(
        geo_api_gouv_commune_contour=pd.DataFrame({"city_code": [], "geometry_wkt": []})
    )

    with pytest.raises(ValueError, match=r"1 communes without geometry.*\['98612'\]"):
        validate(extracts)


def test_validate_ignores_uninhabited_territories():
    extracts = _extracts(
        insee_cog_commune_comer=pd.DataFrame(
            {"com_comer": ["97502", "98901"], "nature_zonage": ["COM", "CPT"]}
        )
    )

    validate(extracts)


def test_validate_fails_on_iris_without_geometry():
    extracts = _extracts(
        ign_contour_iris=pd.DataFrame(
            {
                "iris_code": ["593500101", "975020000"],
                "city_code": ["59350", "97502"],
                "geometry_wkt": ["POINT (0 0)", None],
            }
        )
    )

    with pytest.raises(ValueError, match="1 rows without geometry"):
        validate(extracts)
