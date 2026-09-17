import pandas as pd
import pyarrow as pa
import shapely
from shapely.geometry import shape


def parse_contour_iris(table: pa.Table) -> pd.DataFrame:
    """IGN Contours IRIS GeoParquet (WGS84) -> one row per IRIS with WKT geometry."""
    df = table.select(
        ["code_iris", "nom_iris", "type_iris", "code_insee", "nom_commune", "geometrie"]
    ).to_pandas()
    df["geometry_wkt"] = [
        shapely.to_wkt(shapely.from_wkb(wkb)) if wkb is not None else None
        for wkb in df["geometrie"]
    ]
    return df.rename(
        columns={
            "code_iris": "iris_code",
            "nom_iris": "iris_name",
            "type_iris": "iris_type",
            "code_insee": "city_code",
            "nom_commune": "city_name",
        }
    )[["iris_code", "iris_name", "iris_type", "city_code", "city_name", "geometry_wkt"]]


def parse_cog_table(raw: pd.DataFrame) -> pd.DataFrame:
    """INSEE COG csv (v_commune, v_canton, ...): keep INSEE column names, lowercased."""
    df = raw.rename(columns=str.lower).astype(object)
    return df.where(df.notna() & (df != ""), None)


def parse_epci_communes(raw: pd.DataFrame) -> pd.DataFrame:
    """INSEE 'Intercommunalité' workbook, sheet Composition_communale."""
    return raw.rename(
        columns={
            "CODGEO": "city_code",
            "LIBGEO": "city_name",
            "EPCI": "epci_code",
            "LIBEPCI": "epci_name",
            "DEP": "department_code",
            "REG": "region_code",
        }
    )[
        [
            "city_code",
            "city_name",
            "epci_code",
            "epci_name",
            "department_code",
            "region_code",
        ]
    ]


def parse_epci(raw: pd.DataFrame) -> pd.DataFrame:
    """INSEE 'Intercommunalité' workbook, sheet EPCI."""
    df = raw.rename(
        columns={
            "EPCI": "epci_code",
            "LIBEPCI": "epci_name",
            "NATURE_EPCI": "epci_type",
            "NB_COM": "city_count",
        }
    )[["epci_code", "epci_name", "epci_type", "city_count"]]
    df["city_count"] = df["city_count"].astype(float).astype(int)
    return df


def parse_density_grid(raw: pd.DataFrame) -> pd.DataFrame:
    """INSEE 'grille communale de densité' (7 levels)."""
    df = raw.rename(
        columns={
            "CODGEO": "city_code",
            "LIBGEO": "city_name",
            "DENS": "density_level",
            "LIBDENS": "density_label",
        }
    )[["city_code", "city_name", "density_level", "density_label"]]
    df["density_level"] = df["density_level"].astype(int)
    return df


def parse_zrr(raw: pd.DataFrame) -> pd.DataFrame:
    """ANCT ZRR classification: 'C - Classée en ZRR' -> code 'C', label 'Classée en ZRR'."""
    df = raw.rename(
        columns={
            "CODGEO": "city_code",
            "LIBGEO": "city_name",
            "ZONAGE_ZRR": "zrr_detail",
        }
    )
    code_and_label = df["ZRR_SIMP"].str.split(" - ", n=1, expand=True)
    df["zrr_code"] = code_and_label[0].str.strip()
    df["zrr_label"] = code_and_label[1].str.strip()
    return df[["city_code", "city_name", "zrr_code", "zrr_label", "zrr_detail"]]


def parse_frr(raw: pd.DataFrame) -> pd.DataFrame:
    """DGCL FRR classification (Observatoire des territoires export)."""
    df = raw.rename(
        columns={"codgeo": "city_code", "libgeo": "city_name", "codefrr": "frr_code"}
    )[["city_code", "city_name", "frr_code"]].astype(object)
    return df.where(df.notna(), None)


def parse_geo_api_communes(records: list[dict]) -> pd.DataFrame:
    """geo.api.gouv.fr /communes with contour -> one row per commune with WKT geometry."""
    rows = []
    for record in records:
        if not record.get("contour"):
            raise ValueError(f"commune {record.get('code')} has no contour")
        rows.append(
            {
                "city_code": record["code"],
                "city_name": record["nom"],
                "department_code": record.get("codeDepartement"),
                "region_code": record.get("codeRegion"),
                "geometry_wkt": shape(record["contour"]).wkt,
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "city_code",
            "city_name",
            "department_code",
            "region_code",
            "geometry_wkt",
        ],
    )
