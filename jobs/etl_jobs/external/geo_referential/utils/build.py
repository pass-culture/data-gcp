from collections.abc import Callable

import pandas as pd

# COG v_commune_comer NATURE_ZONAGE: COM = commune, CIR = circonscription (Wallis-et-Futuna)
INHABITED_COMER_ZONING = ["COM", "CIR"]

NO_EPCI_CODE = "ZZZZZZZZZ"
NO_EPCI_LABEL = "Sans objet"

ZRR_PARTIAL = {
    "zrr_code": "P",
    "zrr_label": "Commune partiellement classée en ZRR",
    "zrr_detail": "P - Commune partiellement classée en ZRR",
}

GEO_COMMUNE_COLUMNS = [
    "city_code",
    "city_label",
    "commune_code",
    "department_code",
    "region_code",
    "territorial_authority_code",
    "territorial_authority_label",
    "district_code",
    "district_label",
    "sub_district_code",
    "sub_district_label",
    "epci_code",
    "epci_label",
    "density_level",
    "density_label",
    "zrr_code",
    "zrr_label",
    "zrr_detail",
    "frr_code",
]


def build_geo_iris(
    contour_iris: pd.DataFrame, overseas_communes: pd.DataFrame
) -> pd.DataFrame:
    """One row per IRIS. Overseas collectivities without IRIS get one pseudo-IRIS per commune
    (INSEE convention: commune code + "0000", type Z = commune non irisée)."""
    iris = contour_iris.rename(columns={"iris_name": "iris_label"})
    pseudo_iris = pd.DataFrame(
        {
            "iris_code": overseas_communes["city_code"] + "0000",
            "iris_label": overseas_communes["city_name"],
            "iris_type": "Z",
            "city_code": overseas_communes["city_code"],
            "geometry_wkt": overseas_communes["geometry_wkt"],
        }
    )
    columns = ["iris_code", "iris_label", "iris_type", "city_code", "geometry_wkt"]
    return pd.concat([iris[columns], pseudo_iris[columns]], ignore_index=True)


def commune_predecessors(
    mvt_commune: pd.DataFrame, since_year: int
) -> dict[str, list[str]]:
    """Commune code -> codes it comes from (merged or re-established communes), for the
    COG movements effective after `since_year`."""
    recent = mvt_commune[
        (mvt_commune["date_eff"] > f"{since_year}-12-31")
        & (mvt_commune["com_av"] != mvt_commune["com_ap"])
    ]
    return (
        recent.groupby("com_ap")["com_av"]
        .agg(lambda codes: sorted(set(codes)))
        .to_dict()
    )


def fill_missing_communes(
    attributes: pd.DataFrame,
    current_city_codes: pd.Series,
    predecessors: dict[str, list[str]],
    combine: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """Align an attribute table published for an older COG on the current commune codes:
    a current commune with no row of its own inherits from its predecessors (the communes
    it merged, or the commune it was re-established from), `combine` picking one row out of
    several. Rows of codes that no longer exist are dropped."""
    by_code = attributes.set_index("city_code")
    known = attributes[attributes["city_code"].isin(set(current_city_codes))]
    picked = []
    for city_code in set(current_city_codes) - set(known["city_code"]):
        rows = by_code.loc[by_code.index.isin(_former_codes(city_code, predecessors))]
        if rows.empty:
            continue
        row = combine(rows.reset_index(drop=True)).head(1).copy()
        row["city_code"] = city_code
        picked.append(row)
    return pd.concat([known, *picked], ignore_index=True)[known.columns]


def _former_codes(
    city_code: str, predecessors: dict[str, list[str]], depth: int = 0
) -> set[str]:
    if city_code not in predecessors or depth > 10:
        return set()
    former = set()
    for code in predecessors[city_code]:
        former.add(code)
        former |= _former_codes(code, predecessors, depth + 1)
    return former


def _by_commune(attributes: pd.DataFrame) -> pd.DataFrame:
    return attributes.rename(columns={"city_code": "commune_code"})


def _combine_zrr(rows: pd.DataFrame) -> pd.DataFrame:
    if rows["zrr_code"].nunique() == 1:
        return rows.head(1)
    return pd.DataFrame([ZRR_PARTIAL])


def _combine_densest(rows: pd.DataFrame) -> pd.DataFrame:
    return rows.nsmallest(1, "density_level")


def build_geo_commune(
    cog_commune: pd.DataFrame,
    cog_commune_comer: pd.DataFrame,
    cog_arrondissement: pd.DataFrame,
    cog_canton: pd.DataFrame,
    cog_ctcd: pd.DataFrame,
    epci_communes: pd.DataFrame,
    density_grid: pd.DataFrame,
    zrr: pd.DataFrame,
    frr: pd.DataFrame,
    predecessors: dict[str, list[str]],
) -> pd.DataFrame:
    """One row per commune and per arrondissement (Paris, Lyon, Marseille), including the
    inhabited overseas collectivities. Municipal attributes (EPCI, density, zonings) are those
    of `commune_code`: the parent commune for arrondissements, the commune itself otherwise."""
    cities = cog_commune[cog_commune["typecom"].isin(["COM", "ARM"])].rename(
        columns={
            "com": "city_code",
            "reg": "region_code",
            "dep": "department_code",
            "ctcd": "territorial_authority_code",
            "arr": "district_code",
            "can": "sub_district_code",
        }
    )
    cities = cities.assign(commune_code=cities["comparent"].fillna(cities["city_code"]))

    comer = cog_commune_comer[
        cog_commune_comer["nature_zonage"].isin(INHABITED_COMER_ZONING)
    ]
    comer = pd.DataFrame(
        {
            "city_code": comer["com_comer"],
            "commune_code": comer["com_comer"],
            "libelle": comer["libelle"],
            "department_code": comer["comer"],
        }
    )
    cities = pd.concat([cities, comer], ignore_index=True)

    commune_labels = cities.loc[
        cities["city_code"] == cities["commune_code"], ["commune_code", "libelle"]
    ].rename(columns={"libelle": "city_label"})

    current_codes = cities["commune_code"].drop_duplicates()
    epci = fill_missing_communes(
        epci_communes[["city_code", "epci_code", "epci_name"]],
        current_codes,
        predecessors,
        combine=lambda rows: rows.head(1),
    ).rename(columns={"epci_name": "epci_label"})
    density = fill_missing_communes(
        density_grid[["city_code", "density_level", "density_label"]],
        current_codes,
        predecessors,
        combine=_combine_densest,
    )
    zrr = fill_missing_communes(
        zrr[["city_code", "zrr_code", "zrr_label", "zrr_detail"]],
        current_codes,
        predecessors,
        combine=_combine_zrr,
    )
    frr = fill_missing_communes(
        frr[["city_code", "frr_code"]],
        current_codes,
        predecessors,
        combine=lambda r: r.head(1),
    )

    df = (
        cities.merge(commune_labels, on="commune_code", how="left")
        .merge(_by_commune(epci), on="commune_code", how="left")
        .merge(_by_commune(density), on="commune_code", how="left")
        .merge(_by_commune(zrr), on="commune_code", how="left")
        .merge(_by_commune(frr), on="commune_code", how="left")
        .merge(
            cog_arrondissement[["arr", "libelle"]].rename(
                columns={"arr": "district_code", "libelle": "district_label"}
            ),
            on="district_code",
            how="left",
        )
        .merge(
            cog_canton[["can", "libelle"]].rename(
                columns={"can": "sub_district_code", "libelle": "sub_district_label"}
            ),
            on="sub_district_code",
            how="left",
        )
        .merge(
            cog_ctcd[["ctcd", "libelle"]].rename(
                columns={
                    "ctcd": "territorial_authority_code",
                    "libelle": "territorial_authority_label",
                }
            ),
            on="territorial_authority_code",
            how="left",
        )
    )
    df["epci_code"] = df["epci_code"].fillna(NO_EPCI_CODE)
    df["epci_label"] = df["epci_label"].fillna(NO_EPCI_LABEL)
    df["density_level"] = df["density_level"].astype("Int64")
    return df.reindex(columns=GEO_COMMUNE_COLUMNS)
