import pandas as pd
import pyarrow as pa
import pytest
from shapely.geometry import MultiPolygon, Polygon

from utils.parsers import (
    parse_cog_table,
    parse_contour_iris,
    parse_density_grid,
    parse_epci,
    parse_epci_communes,
    parse_frr,
    parse_geo_api_communes,
    parse_zrr,
    read_table,
)

SQUARE = MultiPolygon([Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])])


def _contour_iris_table(rows: list[dict]) -> pa.Table:
    return pa.table(
        {
            "fid": pa.array([r.get("fid", i) for i, r in enumerate(rows)], pa.int64()),
            "cleabs": [r.get("cleabs", "IRIS_x") for r in rows],
            "code_insee": [r["code_insee"] for r in rows],
            "nom_commune": [r["nom_commune"] for r in rows],
            "iris": [r["iris"] for r in rows],
            "code_iris": [r["code_iris"] for r in rows],
            "nom_iris": [r["nom_iris"] for r in rows],
            "type_iris": [r["type_iris"] for r in rows],
            "geometrie": pa.array([r["geometrie"] for r in rows], pa.binary()),
        }
    )


def test_parse_contour_iris_converts_wkb_to_wkt_and_keeps_codes():
    table = _contour_iris_table(
        [
            {
                "code_insee": "59350",
                "nom_commune": "Lille",
                "iris": "0101",
                "code_iris": "593500101",
                "nom_iris": "Vieux-Lille 1",
                "type_iris": "H",
                "geometrie": SQUARE.wkb,
            }
        ]
    )

    df = parse_contour_iris(table)

    assert list(df.columns) == [
        "iris_code",
        "iris_name",
        "iris_type",
        "city_code",
        "city_name",
        "geometry_wkt",
    ]
    row = df.iloc[0]
    assert row["iris_code"] == "593500101"
    assert row["city_code"] == "59350"
    assert row["geometry_wkt"].startswith("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))")


def test_parse_contour_iris_null_geometry_becomes_none():
    table = _contour_iris_table(
        [
            {
                "code_insee": "97353",
                "nom_commune": "Maripasoula",
                "iris": "0101",
                "code_iris": "973530101",
                "nom_iris": "Centre-Bourg",
                "type_iris": "H",
                "geometrie": None,
            }
        ]
    )

    df = parse_contour_iris(table)

    assert df.iloc[0]["geometry_wkt"] is None


def test_parse_cog_table_lowercases_columns_and_keeps_strings():
    raw = pd.DataFrame(
        {
            "TYPECOM": ["COM", "COMD"],
            "COM": ["01001", "02077"],
            "REG": ["84", "32"],
            "DEP": ["01", "02"],
            "LIBELLE": ["L'Abergement-Clémenciat", "Berzy-le-Sec"],
            "COMPARENT": ["", "02564"],
        }
    )

    df = parse_cog_table(raw)

    assert list(df.columns) == ["typecom", "com", "reg", "dep", "libelle", "comparent"]
    assert df["com"].tolist() == ["01001", "02077"]
    assert df["comparent"].tolist() == [None, "02564"]


def test_parse_epci_communes_renames_insee_columns():
    raw = pd.DataFrame(
        {
            "CODGEO": ["59350"],
            "LIBGEO": ["Lille"],
            "EPCI": ["200093201"],
            "LIBEPCI": ["Métropole Européenne de Lille"],
            "DEP": ["59"],
            "REG": ["32"],
        }
    )

    df = parse_epci_communes(raw)

    assert df.to_dict(orient="records") == [
        {
            "city_code": "59350",
            "city_name": "Lille",
            "epci_code": "200093201",
            "epci_name": "Métropole Européenne de Lille",
            "department_code": "59",
            "region_code": "32",
        }
    ]


def test_parse_epci_casts_commune_count_to_int():
    raw = pd.DataFrame(
        {
            "EPCI": ["200093201"],
            "LIBEPCI": ["Métropole Européenne de Lille"],
            "NATURE_EPCI": ["ME"],
            "NB_COM": ["95.0"],
        }
    )

    df = parse_epci(raw)

    assert df.to_dict(orient="records") == [
        {
            "epci_code": "200093201",
            "epci_name": "Métropole Européenne de Lille",
            "epci_type": "ME",
            "city_count": 95,
        }
    ]


def test_parse_density_grid_keeps_level_and_label():
    raw = pd.DataFrame(
        {
            "CODGEO": ["01001"],
            "LIBGEO": ["L'Abergement-Clémenciat"],
            "DENS": ["6"],
            "LIBDENS": ["Rural à habitat dispersé"],
            "PMUN21": ["832"],
            "P1": ["0"],
        }
    )

    df = parse_density_grid(raw)

    assert df.to_dict(orient="records") == [
        {
            "city_code": "01001",
            "city_name": "L'Abergement-Clémenciat",
            "density_level": 6,
            "density_label": "Rural à habitat dispersé",
        }
    ]


def test_parse_zrr_normalizes_labels():
    raw = pd.DataFrame(
        {
            "CODGEO": ["01001", "01002"],
            "LIBGEO": ["A", "B"],
            "ZRR_SIMP": ["C - Classée en ZRR", "NC - Commune non classée"],
            "ZONAGE_ZRR": ["Classée en ZRR", "Commune non classée"],
        }
    )

    df = parse_zrr(raw)

    assert df.to_dict(orient="records") == [
        {
            "city_code": "01001",
            "city_name": "A",
            "zrr_code": "C",
            "zrr_label": "Classée en ZRR",
            "zrr_detail": "Classée en ZRR",
        },
        {
            "city_code": "01002",
            "city_name": "B",
            "zrr_code": "NC",
            "zrr_label": "Commune non classée",
            "zrr_detail": "Commune non classée",
        },
    ]


def test_parse_frr_keeps_null_code_for_unclassified():
    raw = pd.DataFrame(
        {
            "codgeo": ["01001", "01187", "97401"],
            "libgeo": ["A", "Haut Valromey", "Les Avirons"],
            "codefrr": [None, "2", "3"],
        }
    )

    df = parse_frr(raw)

    assert df.to_dict(orient="records") == [
        {"city_code": "01001", "city_name": "A", "frr_code": None},
        {"city_code": "01187", "city_name": "Haut Valromey", "frr_code": "2"},
        {"city_code": "97401", "city_name": "Les Avirons", "frr_code": "3"},
    ]


def test_parse_geo_api_communes_converts_geojson_to_wkt():
    records = [
        {
            "code": "98612",
            "nom": "Sigave",
            "codeDepartement": "986",
            "codeRegion": "986",
            "contour": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
            },
        }
    ]

    df = parse_geo_api_communes(records)

    assert list(df.columns) == [
        "city_code",
        "city_name",
        "department_code",
        "region_code",
        "geometry_wkt",
    ]
    assert df.iloc[0]["city_code"] == "98612"
    assert df.iloc[0]["geometry_wkt"].startswith("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")


def test_parse_geo_api_communes_rejects_missing_contour():
    records = [{"code": "98612", "nom": "Sigave", "codeDepartement": "986"}]

    with pytest.raises(ValueError, match="98612"):
        parse_geo_api_communes(records)


def test_read_table_finds_header_row_and_returns_strings(insee_like_xlsx):
    df = read_table(insee_like_xlsx, sheet="Composition_communale", header_key="CODGEO")

    assert list(df.columns) == ["CODGEO", "LIBGEO", "EPCI", "LIBEPCI", "DEP", "REG"]
    assert len(df) == 3
    assert df.iloc[0].tolist() == [
        "59350",
        "Lille",
        "200093201",
        "Métropole Européenne de Lille",
        "59",
        "32",
    ]
    assert df["CODGEO"].dtype == object


def test_read_table_keeps_leading_zeros(insee_like_xlsx):
    df = read_table(insee_like_xlsx, sheet="Composition_communale", header_key="CODGEO")

    assert "01001" in df["CODGEO"].tolist()


def test_read_table_raises_when_header_key_missing(insee_like_xlsx):
    with pytest.raises(ValueError, match="header 'NOPE' not found"):
        read_table(insee_like_xlsx, sheet="Composition_communale", header_key="NOPE")
