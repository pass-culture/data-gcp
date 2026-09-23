import pandas as pd
import pytest

from utils.checks import (
    check_all_cities_have_geometry,
    check_epci_codes_known,
    check_no_empty_geometry,
    check_unique,
)


def test_check_no_empty_geometry_passes_when_all_filled():
    df = pd.DataFrame(
        {"iris_code": ["a", "b"], "geometry_wkt": ["POINT (0 0)", "POINT (1 1)"]}
    )

    check_no_empty_geometry(df, key="iris_code")


def test_check_no_empty_geometry_lists_offending_keys():
    df = pd.DataFrame(
        {"iris_code": ["a", "b", "c"], "geometry_wkt": ["POINT (0 0)", None, ""]}
    )

    with pytest.raises(ValueError, match=r"2 rows without geometry.*\['b', 'c'\]"):
        check_no_empty_geometry(df, key="iris_code")


def test_check_unique_raises_on_duplicates():
    df = pd.DataFrame({"iris_code": ["a", "a", "b"]})

    with pytest.raises(ValueError, match=r"iris_code.*\['a'\]"):
        check_unique(df, "iris_code")


def test_check_all_cities_have_geometry_passes_when_covered():
    cities = pd.DataFrame(
        {"typecom": ["COM", "COM", "COMD"], "com": ["59350", "97302", "02077"]}
    )
    covered = pd.Series(["59350", "97302"])

    check_all_cities_have_geometry(cities, covered)


def test_check_all_cities_have_geometry_credits_parent_of_covered_arrondissements():
    cities = pd.DataFrame(
        {
            "typecom": ["COM", "ARM", "ARM"],
            "com": ["75056", "75101", "75102"],
            "comparent": [None, "75056", "75056"],
        }
    )
    covered = pd.Series(["75101", "75102"])

    check_all_cities_have_geometry(cities, covered)


def test_check_all_cities_have_geometry_reports_missing_communes():
    cities = pd.DataFrame({"typecom": ["COM", "COM"], "com": ["59350", "97302"]})
    covered = pd.Series(["59350"])

    with pytest.raises(ValueError, match=r"1 communes without geometry.*\['97302'\]"):
        check_all_cities_have_geometry(cities, covered)


def test_check_epci_codes_known_reports_unknown_codes():
    epci_communes = pd.DataFrame({"city_code": ["a", "b"], "epci_code": ["1", "2"]})
    epci = pd.DataFrame({"epci_code": ["1"]})

    with pytest.raises(ValueError, match=r"1 EPCI codes.*\['2'\]"):
        check_epci_codes_known(epci_communes, epci)
