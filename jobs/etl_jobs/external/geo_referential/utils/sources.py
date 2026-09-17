from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from utils import config
from utils.download import download, download_json, extract_member, single_member
from utils.excel import read_table
from utils.parsers import (
    parse_cog_table,
    parse_contour_iris,
    parse_density_grid,
    parse_epci,
    parse_epci_communes,
    parse_frr,
    parse_geo_api_communes,
    parse_zrr,
)

ZRR_VINTAGE_YEAR = 2021

COG_TABLES = {
    "insee_cog_commune": "v_commune_{year}.csv",
    "insee_cog_commune_comer": "v_commune_comer_{year}.csv",
    "insee_cog_arrondissement": "v_arrondissement_{year}.csv",
    "insee_cog_canton": "v_canton_{year}.csv",
    "insee_cog_ctcd": "v_ctcd_{year}.csv",
    "insee_cog_mvt_commune": "v_mvt_commune_{year}.csv",
}


@dataclass(frozen=True)
class Vintages:
    year: int
    density_year: int
    frr_year: int


@dataclass(frozen=True)
class Extract:
    table_name: str
    vintage_year: int
    df: pd.DataFrame


def load_contour_iris(vintages: Vintages, download_dir: Path) -> Extract:
    path = download(
        config.contour_iris_url(vintages.year),
        download_dir / f"contours_iris_{vintages.year}.parquet",
    )
    return Extract(
        "ign_contour_iris", vintages.year, parse_contour_iris(pq.read_table(path))
    )


def load_cog(vintages: Vintages, download_dir: Path) -> list[Extract]:
    zip_path = download(
        config.cog_url(vintages.year), download_dir / f"cog_{vintages.year}.zip"
    )
    extracts = []
    for table_name, member in COG_TABLES.items():
        csv_path = extract_member(
            zip_path, member.format(year=vintages.year), download_dir
        )
        raw = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        extracts.append(Extract(table_name, vintages.year, parse_cog_table(raw)))
    return extracts


def load_epci(vintages: Vintages, download_dir: Path) -> list[Extract]:
    zip_path = download(
        config.epci_url(vintages.year), download_dir / f"epci_{vintages.year}.zip"
    )
    xlsx_path = extract_member(zip_path, single_member(zip_path, ".xlsx"), download_dir)
    return [
        Extract(
            "insee_epci",
            vintages.year,
            parse_epci(read_table(xlsx_path, sheet="EPCI", header_key="EPCI")),
        ),
        Extract(
            "insee_epci_commune",
            vintages.year,
            parse_epci_communes(
                read_table(
                    xlsx_path, sheet="Composition_communale", header_key="CODGEO"
                )
            ),
        ),
    ]


def load_density_grid(vintages: Vintages, download_dir: Path) -> Extract:
    path = download(
        config.density_grid_url(vintages.density_year),
        download_dir / f"density_grid_{vintages.density_year}.xlsx",
    )
    raw = read_table(path, sheet="Grille_Densite", header_key="CODGEO")
    return Extract("insee_density_grid", vintages.density_year, parse_density_grid(raw))


def load_zrr(vintages: Vintages, download_dir: Path) -> Extract:
    path = download(config.ZRR_URL, download_dir / "zrr_cog2021.xls")
    raw = read_table(path, sheet=config.ZRR_SHEET, header_key="CODGEO")
    return Extract("anct_zrr", ZRR_VINTAGE_YEAR, parse_zrr(raw))


def load_frr(vintages: Vintages, download_dir: Path) -> Extract:
    path = download(
        config.frr_url(vintages.frr_year),
        download_dir / f"frr_{vintages.frr_year}.xlsx",
    )
    raw = read_table(path, sheet=config.FRR_SHEET, header_key="codgeo")
    return Extract("dgcl_frr", vintages.frr_year, parse_frr(raw))


def load_geo_api_communes(vintages: Vintages, download_dir: Path) -> Extract:
    records = []
    for department_code in config.GEO_API_DEPARTMENTS:
        records.extend(download_json(config.geo_api_communes_url(department_code)))
    return Extract(
        "geo_api_gouv_commune_contour", vintages.year, parse_geo_api_communes(records)
    )


def load_all(vintages: Vintages, download_dir: Path) -> dict[str, Extract]:
    extracts = [
        load_contour_iris(vintages, download_dir),
        *load_cog(vintages, download_dir),
        *load_epci(vintages, download_dir),
        load_density_grid(vintages, download_dir),
        load_zrr(vintages, download_dir),
        load_frr(vintages, download_dir),
        load_geo_api_communes(vintages, download_dir),
    ]
    return {extract.table_name: extract for extract in extracts}
