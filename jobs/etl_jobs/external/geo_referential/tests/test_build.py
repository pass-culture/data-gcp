import pandas as pd

from utils.build import (
    GEO_MUNICIPALITY_COLUMNS,
    build_geo_iris,
    build_geo_municipality,
    fill_missing_municipalities,
    municipality_predecessors,
)


def test_build_geo_iris_appends_one_pseudo_iris_per_overseas_commune():
    contour_iris = pd.DataFrame(
        {
            "iris_code": ["593500101"],
            "iris_name": ["Vieux-Lille 1"],
            "iris_type": ["H"],
            "city_code": ["59350"],
            "city_name": ["Lille"],
            "geometry_wkt": ["MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))"],
        }
    )
    overseas = pd.DataFrame(
        {
            "city_code": ["98818"],
            "city_name": ["Nouméa"],
            "department_code": ["988"],
            "region_code": ["988"],
            "geometry_wkt": ["POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"],
        }
    )

    df = build_geo_iris(contour_iris, overseas)

    assert list(df.columns) == [
        "iris_code",
        "iris_label",
        "iris_type",
        "city_code",
        "geometry_wkt",
    ]
    assert df.to_dict(orient="records")[1] == {
        "iris_code": "988180000",
        "iris_label": "Nouméa",
        "iris_type": "Z",
        "city_code": "98818",
        "geometry_wkt": "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
    }


def test_municipality_predecessors_lists_former_codes_after_the_vintage():
    mvt = pd.DataFrame(
        {
            "mod": ["32", "32", "21", "10", "32"],
            "date_eff": [
                "2022-01-01",
                "2022-01-01",
                "2024-01-01",
                "2023-01-01",
                "2019-01-01",
            ],
            "com_av": ["01001", "01002", "15084", "01003", "01009"],
            "com_ap": ["01005", "01005", "15031", "01003", "01010"],
        }
    )

    predecessors = municipality_predecessors(mvt, since_year=2021)

    assert predecessors == {"01005": ["01001", "01002"], "15031": ["15084"]}


def test_fill_missing_municipalities_inherits_from_merged_communes():
    density = pd.DataFrame(
        {"city_code": ["01001", "01002", "01003"], "density_level": [6, 3, 5]}
    )
    current = pd.Series(["01005", "01003"])
    predecessors = {"01005": ["01001", "01002"]}

    df = fill_missing_municipalities(
        density,
        current,
        predecessors,
        combine=lambda rows: rows.nsmallest(1, "density_level"),
    )

    assert df.sort_values("city_code").to_dict(orient="records") == [
        {"city_code": "01003", "density_level": 5},
        {"city_code": "01005", "density_level": 3},
    ]


def test_fill_missing_municipalities_inherits_from_a_still_existing_predecessor():
    # Re-established commune: the merged commune keeps its code and its row.
    zrr = pd.DataFrame({"city_code": ["15084"], "zrr_code": ["C"]})
    current = pd.Series(["15084", "15031"])
    predecessors = {"15031": ["15084"]}

    df = fill_missing_municipalities(
        zrr, current, predecessors, combine=lambda rows: rows.head(1)
    )

    assert df.sort_values("city_code").to_dict(orient="records") == [
        {"city_code": "15031", "zrr_code": "C"},
        {"city_code": "15084", "zrr_code": "C"},
    ]


def test_fill_missing_municipalities_follows_chains_and_keeps_existing_rows():
    zrr = pd.DataFrame({"city_code": ["01001", "01002"], "zrr_code": ["C", "NC"]})
    current = pd.Series(["01002", "01009"])
    predecessors = {"01009": ["01007"], "01007": ["01001"], "01002": ["01001"]}

    df = fill_missing_municipalities(
        zrr, current, predecessors, combine=lambda rows: rows.head(1)
    )

    assert df.sort_values("city_code").to_dict(orient="records") == [
        {"city_code": "01002", "zrr_code": "NC"},
        {"city_code": "01009", "zrr_code": "C"},
    ]


def _cog():
    commune = pd.DataFrame(
        {
            "typecom": ["COM", "COM", "ARM", "COMD"],
            "com": ["59350", "75056", "75101", "02077"],
            "reg": ["32", "11", "11", "32"],
            "dep": ["59", "75", "75", "02"],
            "ctcd": ["59D", "75C", "75C", "02D"],
            "arr": ["595", "751", "751", "023"],
            "can": ["5997", "7599", "7599", "0299"],
            "libelle": ["Lille", "Paris", "Paris 1er Arrondissement", "Berzy-le-Sec"],
            "comparent": [None, None, "75056", "02564"],
        }
    )
    comer = pd.DataFrame(
        {
            "com_comer": ["98818", "98412"],
            "libelle": ["Nouméa", "Archipel des Kerguelen"],
            "nature_zonage": ["COM", "DIS"],
            "comer": ["988", "984"],
        }
    )
    arrondissement = pd.DataFrame(
        {"arr": ["595", "751"], "libelle": ["Lille", "Paris"]}
    )
    canton = pd.DataFrame({"can": ["5997", "7599"], "libelle": ["Lille-1", "Paris"]})
    ctcd = pd.DataFrame({"ctcd": ["59D", "75C"], "libelle": ["Nord", "Ville de Paris"]})
    return commune, comer, arrondissement, canton, ctcd


def test_build_geo_municipality_arrondissement_takes_parent_label_and_attributes():
    commune, comer, arrondissement, canton, ctcd = _cog()
    epci = pd.DataFrame(
        {
            "city_code": ["59350", "75056"],
            "epci_code": ["200093201", "200054781"],
            "epci_name": ["Métropole Européenne de Lille", "Métropole du Grand Paris"],
        }
    )
    density = pd.DataFrame(
        {
            "city_code": ["59350", "75056"],
            "density_level": [1, 1],
            "density_label": ["Grands centres urbains", "Grands centres urbains"],
        }
    )
    zrr = pd.DataFrame(
        {
            "city_code": ["59350", "75056"],
            "zrr_code": ["NC", "NC"],
            "zrr_label": ["Commune non classée", "Commune non classée"],
            "zrr_detail": ["Commune non classée", "Commune non classée"],
        }
    )
    frr = pd.DataFrame({"city_code": ["59350", "75056"], "frr_code": [None, None]})

    df = build_geo_municipality(
        commune,
        comer,
        arrondissement,
        canton,
        ctcd,
        epci,
        density,
        zrr,
        frr,
        predecessors={},
    )

    assert list(df.columns) == GEO_MUNICIPALITY_COLUMNS
    paris_1 = df[df.city_code == "75101"].iloc[0]
    assert paris_1["municipality_code"] == "75056"
    assert paris_1["municipality_label"] == "Paris"
    assert paris_1["epci_code"] == "200054781"
    assert paris_1["density_level"] == 1
    assert paris_1["territorial_authority_label"] == "Ville de Paris"
    assert paris_1["district_label"] == "Paris"
    assert paris_1["sub_district_label"] == "Paris"
    assert "02077" not in df.city_code.tolist()


def test_build_geo_municipality_overseas_commune_has_no_epci_and_no_density():
    commune, comer, arrondissement, canton, ctcd = _cog()
    empty = pd.DataFrame({"city_code": []})

    df = build_geo_municipality(
        commune,
        comer,
        arrondissement,
        canton,
        ctcd,
        empty.assign(epci_code=[], epci_name=[]),
        empty.assign(density_level=[], density_label=[]),
        empty.assign(zrr_code=[], zrr_label=[], zrr_detail=[]),
        empty.assign(frr_code=[]),
        predecessors={},
    )

    noumea = df[df.city_code == "98818"].iloc[0]
    assert noumea["municipality_label"] == "Nouméa"
    assert noumea["department_code"] == "988"
    assert noumea["epci_code"] == "ZZZZZZZZZ"
    assert noumea["epci_label"] == "Sans objet"
    assert pd.isna(noumea["density_level"])
    assert "98412" not in df.city_code.tolist()


def test_build_geo_municipality_merged_commune_with_mixed_zrr_is_partially_classified():
    commune, comer, arrondissement, canton, ctcd = _cog()
    empty = pd.DataFrame({"city_code": []})
    zrr = pd.DataFrame(
        {
            "city_code": ["59001", "59002"],
            "zrr_code": ["C", "NC"],
            "zrr_label": ["Classée en ZRR", "Commune non classée"],
            "zrr_detail": ["C - Commune classée en ZRR", "NC - Commune non classée"],
        }
    )

    df = build_geo_municipality(
        commune,
        comer,
        arrondissement,
        canton,
        ctcd,
        empty.assign(epci_code=[], epci_name=[]),
        empty.assign(density_level=[], density_label=[]),
        zrr,
        empty.assign(frr_code=[]),
        predecessors={"59350": ["59001", "59002"]},
    )

    lille = df[df.city_code == "59350"].iloc[0]
    assert lille["zrr_code"] == "P"
    assert lille["zrr_label"] == "Commune partiellement classée en ZRR"
