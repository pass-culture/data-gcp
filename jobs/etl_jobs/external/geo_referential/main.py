import logging
import tempfile
from pathlib import Path

import pandas as pd
import typer

from utils.bigquery import save
from utils.build import (
    INHABITED_COMER_ZONING,
    build_geo_iris,
    build_geo_municipality,
    municipality_predecessors,
)
from utils.checks import (
    check_all_cities_have_geometry,
    check_epci_codes_known,
    check_no_empty_geometry,
    check_unique,
)
from utils.sources import ZRR_VINTAGE_YEAR, Extract, Vintages, load_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

run = typer.Typer()


def validate(extracts: dict[str, Extract]) -> None:
    contour_iris = extracts["ign_contour_iris"].df
    geo_api_communes = extracts["geo_api_gouv_commune_contour"].df
    cog_communes = extracts["insee_cog_commune"].df
    cog_communes_comer = extracts["insee_cog_commune_comer"].df

    check_unique(contour_iris, "iris_code")
    check_no_empty_geometry(contour_iris, key="iris_code")
    check_unique(geo_api_communes, "city_code")
    check_no_empty_geometry(geo_api_communes, key="city_code")

    # Every commune of the COG (métropole, DROM and overseas collectivities) has a geometry,
    # either from IGN Contours IRIS or from geo.api.gouv.fr. Uninhabited territories
    # (TAAF districts, Clipperton, Île des Faisans) have none.
    covered = pd.concat([contour_iris["city_code"], geo_api_communes["city_code"]])
    check_all_cities_have_geometry(cog_communes, covered)
    inhabited = cog_communes_comer["nature_zonage"].isin(INHABITED_COMER_ZONING)
    comer = (
        cog_communes_comer[inhabited]
        .rename(columns={"com_comer": "com"})
        .assign(typecom="COM")
    )
    check_all_cities_have_geometry(comer, covered)

    check_unique(extracts["insee_epci"].df, "epci_code")
    check_unique(extracts["insee_epci_commune"].df, "city_code")
    check_epci_codes_known(extracts["insee_epci_commune"].df, extracts["insee_epci"].df)


def build(
    extracts: dict[str, Extract], vintages: Vintages
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build the IRIS and municipality referentials from the source extracts."""
    df = {name: extract.df for name, extract in extracts.items()}
    geo_iris = build_geo_iris(
        df["ign_contour_iris"], df["geo_api_gouv_commune_contour"]
    )
    geo_municipality = build_geo_municipality(
        cog_commune=df["insee_cog_commune"],
        cog_commune_comer=df["insee_cog_commune_comer"],
        cog_arrondissement=df["insee_cog_arrondissement"],
        cog_canton=df["insee_cog_canton"],
        cog_ctcd=df["insee_cog_ctcd"],
        epci_municipalities=df["insee_epci_commune"],
        density_grid=df["insee_density_grid"],
        zrr=df["anct_zrr"],
        frr=df["dgcl_frr"],
        predecessors=municipality_predecessors(
            df["insee_cog_mvt_commune"],
            since_year=min(ZRR_VINTAGE_YEAR, vintages.density_year, vintages.frr_year),
        ),
    )
    check_unique(geo_iris, "iris_code")
    check_unique(geo_municipality, "city_code")
    return geo_iris, geo_municipality


def describe(vintages: Vintages) -> str:
    return (
        f"Geographic referential built by import_geo_referential. Vintages: "
        f"IGN Contours IRIS / INSEE COG / INSEE EPCI {vintages.year}, "
        f"INSEE density grid {vintages.density_year}, DGCL FRR {vintages.frr_year}, "
        f"ANCT ZRR {ZRR_VINTAGE_YEAR} (COG 2021, aligned on current commune codes)."
    )


@run.command()
def import_geo_referential(
    year: int = typer.Option(
        ..., help="COG / EPCI / Contours IRIS vintage (e.g. 2026)"
    ),
    density_year: int = typer.Option(
        ..., help="INSEE density grid vintage (e.g. 2024)"
    ),
    frr_year: int = typer.Option(..., help="FRR zoning vintage (e.g. 2025)"),
    destination_dataset_id: str = typer.Option(
        ..., help="Destination dataset id (raw_<env>)"
    ),
    iris_table_name: str = typer.Option(
        ..., help="Destination table for the IRIS referential"
    ),
    municipality_table_name: str = typer.Option(
        ..., help="Destination table for the municipality referential"
    ),
    dry_run: bool = typer.Option(
        False, help="Download and validate without writing to BigQuery"
    ),
) -> None:
    try:
        vintages = Vintages(year=year, density_year=density_year, frr_year=frr_year)
        with tempfile.TemporaryDirectory() as download_dir:
            extracts = load_all(vintages, Path(download_dir))
        for extract in extracts.values():
            logger.info("%s: %s rows", extract.table_name, len(extract.df))
        validate(extracts)

        geo_iris, geo_municipality = build(extracts, vintages)
        tables = {
            iris_table_name: geo_iris,
            municipality_table_name: geo_municipality,
        }
        for table_name, df in tables.items():
            logger.info("%s: %s rows", table_name, len(df))
        if dry_run:
            logger.info("Dry run: nothing written")
            return

        for table_name, df in tables.items():
            save(
                df,
                destination_dataset_id,
                table_name,
                vintages.year,
                describe(vintages),
            )
        logger.info(
            "Done: %s tables written to %s", len(tables), destination_dataset_id
        )
    except typer.Exit:
        raise
    except Exception as e:
        logger.exception(f"ETL job failed: {e}")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    run()
