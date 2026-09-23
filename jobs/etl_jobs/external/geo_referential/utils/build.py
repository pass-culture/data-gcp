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

GEO_MUNICIPALITY_COLUMNS = [
    "city_code",
    "city_label",
    "municipality_label",
    "municipality_code",
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
    contour_iris: pd.DataFrame, overseas_municipalities: pd.DataFrame
) -> pd.DataFrame:
    """One row per IRIS. Overseas collectivities without IRIS get one pseudo-IRIS per municipality
    (INSEE convention: municipality code + "0000", type Z = commune non irisée)."""
    iris = contour_iris.rename(columns={"iris_name": "iris_label"})
    pseudo_iris = pd.DataFrame(
        {
            "iris_code": overseas_municipalities["city_code"] + "0000",
            "iris_label": overseas_municipalities["city_name"],
            "iris_type": "Z",
            "city_code": overseas_municipalities["city_code"],
            "geometry_wkt": overseas_municipalities["geometry_wkt"],
        }
    )
    columns = ["iris_code", "iris_label", "iris_type", "city_code", "geometry_wkt"]
    return pd.concat([iris[columns], pseudo_iris[columns]], ignore_index=True)


def municipality_predecessors(
    mvt_commune: pd.DataFrame, since_year: int
) -> dict[str, list[str]]:
    """Municipality code -> codes it comes from (merged or re-established municipalities), for the
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


def fill_missing_municipalities(
    attributes: pd.DataFrame,
    current_city_codes: pd.Series,
    predecessors: dict[str, list[str]],
    combine: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """Align an attribute table published for an older COG on the current municipality codes:
    a current municipality with no row of its own inherits from its predecessors (the ones
    it merged, or the municipality it was re-established from), `combine` picking one row out of
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


def _join_municipality_attributes(
    cities: pd.DataFrame, attributes: list[pd.DataFrame]
) -> pd.DataFrame:
    """Attach attribute tables keyed by `city_code` to the municipality of each row."""
    df = cities
    for attribute in attributes:
        df = df.merge(
            attribute.rename(columns={"city_code": "municipality_code"}),
            on="municipality_code",
            how="left",
        )
    return df


def _join_cog_labels(
    df: pd.DataFrame, labels: dict[str, tuple[pd.DataFrame, str]]
) -> pd.DataFrame:
    """Attach the label of each COG level, keyed by its code column."""
    for code_column, (cog_table, cog_code_column) in labels.items():
        label_column = code_column.replace("_code", "_label")
        df = df.merge(
            cog_table[[cog_code_column, "libelle"]].rename(
                columns={cog_code_column: code_column, "libelle": label_column}
            ),
            on=code_column,
            how="left",
        )
    return df


def _combine_zrr(rows: pd.DataFrame) -> pd.DataFrame:
    if rows["zrr_code"].nunique() == 1:
        return rows.head(1)
    return pd.DataFrame([ZRR_PARTIAL])


def _combine_densest(rows: pd.DataFrame) -> pd.DataFrame:
    return rows.nsmallest(1, "density_level")


def build_geo_municipality(
    cog_commune: pd.DataFrame,
    cog_commune_comer: pd.DataFrame,
    cog_arrondissement: pd.DataFrame,
    cog_canton: pd.DataFrame,
    cog_ctcd: pd.DataFrame,
    epci_municipalities: pd.DataFrame,
    density_grid: pd.DataFrame,
    zrr: pd.DataFrame,
    frr: pd.DataFrame,
    predecessors: dict[str, list[str]],
) -> pd.DataFrame:
    """One row per municipality and per arrondissement (Paris, Lyon, Marseille), including the
    inhabited overseas collectivities. `city_label` is the row's own name (the arrondissement's for
    arrondissements). Municipal attributes (label, EPCI, density, zonings) are those of
    `municipality_code`: the parent municipality for arrondissements, the municipality itself otherwise."""
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
    cities = cities.assign(
        municipality_code=cities["comparent"].fillna(cities["city_code"])
    )

    comer = cog_commune_comer[
        cog_commune_comer["nature_zonage"].isin(INHABITED_COMER_ZONING)
    ]
    comer = pd.DataFrame(
        {
            "city_code": comer["com_comer"],
            "municipality_code": comer["com_comer"],
            "libelle": comer["libelle"],
            "department_code": comer["comer"],
        }
    )
    cities = pd.concat([cities, comer], ignore_index=True).rename(
        columns={"libelle": "city_label"}
    )

    municipality_labels = cities.loc[
        cities["city_code"] == cities["municipality_code"],
        ["municipality_code", "city_label"],
    ].rename(columns={"city_label": "municipality_label"})

    current_codes = cities["municipality_code"].drop_duplicates()
    epci = fill_missing_municipalities(
        epci_municipalities[["city_code", "epci_code", "epci_name"]],
        current_codes,
        predecessors,
        combine=lambda rows: rows.head(1),
    ).rename(columns={"epci_name": "epci_label"})
    density = fill_missing_municipalities(
        density_grid[["city_code", "density_level", "density_label"]],
        current_codes,
        predecessors,
        combine=_combine_densest,
    )
    zrr = fill_missing_municipalities(
        zrr[["city_code", "zrr_code", "zrr_label", "zrr_detail"]],
        current_codes,
        predecessors,
        combine=_combine_zrr,
    )
    frr = fill_missing_municipalities(
        frr[["city_code", "frr_code"]],
        current_codes,
        predecessors,
        combine=lambda r: r.head(1),
    )

    df = cities.merge(municipality_labels, on="municipality_code", how="left")
    df = _join_municipality_attributes(df, [epci, density, zrr, frr])
    df = _join_cog_labels(
        df,
        {
            "district_code": (cog_arrondissement, "arr"),
            "sub_district_code": (cog_canton, "can"),
            "territorial_authority_code": (cog_ctcd, "ctcd"),
        },
    )
    df["epci_code"] = df["epci_code"].fillna(NO_EPCI_CODE)
    df["epci_label"] = df["epci_label"].fillna(NO_EPCI_LABEL)
    df["density_level"] = df["density_level"].astype("Int64")
    return df.reindex(columns=GEO_MUNICIPALITY_COLUMNS)
