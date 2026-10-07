import pandas as pd

MAX_LISTED = 20


def check_no_empty_geometry(df: pd.DataFrame, key: str) -> None:
    empty = df["geometry_wkt"].isna() | (df["geometry_wkt"] == "")
    if empty.any():
        keys = df.loc[empty, key].tolist()
        raise ValueError(
            f"{empty.sum()} rows without geometry, {key}: {keys[:MAX_LISTED]}"
        )


def check_unique(df: pd.DataFrame, column: str) -> None:
    duplicated = df.loc[df[column].duplicated(), column].unique().tolist()
    if duplicated:
        raise ValueError(
            f"{len(duplicated)} duplicated {column}: {duplicated[:MAX_LISTED]}"
        )


def check_all_cities_have_geometry(
    cog_communes: pd.DataFrame, covered_city_codes: pd.Series
) -> None:
    """Every commune (TYPECOM = COM, i.e. not an arrondissement or a merged commune)
    must have at least one geometry row. Paris, Lyon and Marseille carry their geometry
    on their arrondissements (TYPECOM = ARM), so a covered arrondissement covers its parent."""
    covered = set(covered_city_codes)
    if "comparent" in cog_communes:
        arrondissements = cog_communes[cog_communes["typecom"] == "ARM"]
        covered |= set(
            arrondissements.loc[arrondissements["com"].isin(covered), "comparent"]
        )
    communes = cog_communes.loc[cog_communes["typecom"] == "COM", "com"]
    missing = sorted(set(communes) - covered)
    if missing:
        raise ValueError(
            f"{len(missing)} communes without geometry: {missing[:MAX_LISTED]}"
        )


def check_epci_codes_known(epci_communes: pd.DataFrame, epci: pd.DataFrame) -> None:
    unknown = sorted(set(epci_communes["epci_code"].dropna()) - set(epci["epci_code"]))
    if unknown:
        raise ValueError(
            f"{len(unknown)} EPCI codes not in the EPCI list: {unknown[:MAX_LISTED]}"
        )
