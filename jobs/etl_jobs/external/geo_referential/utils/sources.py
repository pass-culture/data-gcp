import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import requests

from utils import config
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

logger = logging.getLogger(__name__)

ZRR_VINTAGE_YEAR = 2021

COG_TABLES = {
    "insee_cog_commune": "v_commune_{year}.csv",
    "insee_cog_commune_comer": "v_commune_comer_{year}.csv",
    "insee_cog_arrondissement": "v_arrondissement_{year}.csv",
    "insee_cog_canton": "v_canton_{year}.csv",
    "insee_cog_ctcd": "v_ctcd_{year}.csv",
    "insee_cog_mvt_commune": "v_mvt_commune_{year}.csv",
}


def download(url: str, dest: Path) -> Path:
    """Stream `url` to `dest`. The IGN parquet is 130+ MB and the connection regularly
    breaks mid-stream, so truncated downloads are retried, resuming with a range request
    when the server supports it."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.unlink(missing_ok=True)
    for attempt in range(1, config.DOWNLOAD_ATTEMPTS + 1):
        downloaded = dest.stat().st_size if dest.exists() else 0
        logger.info(
            "Downloading %s (attempt %s, from byte %s)", url, attempt, downloaded
        )
        try:
            return _stream_to_file(url, dest, downloaded)
        except OSError as error:
            if attempt == config.DOWNLOAD_ATTEMPTS:
                raise
            logger.warning("Download of %s failed (%s), retrying", url, error)
    raise AssertionError("unreachable")


def _stream_to_file(url: str, dest: Path, downloaded: int) -> Path:
    headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}
    with requests.get(
        url, stream=True, timeout=config.DOWNLOAD_TIMEOUT, headers=headers
    ) as response:
        response.raise_for_status()
        resumed = response.status_code == 206
        expected_size = _expected_size(response, resumed, downloaded)
        with dest.open("ab" if resumed else "wb") as f:
            for chunk in response.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    written = dest.stat().st_size
    if expected_size and written != expected_size:
        raise OSError(f"truncated download from {url}: {written}/{expected_size} bytes")
    return dest


def _expected_size(response: requests.Response, resumed: bool, downloaded: int) -> int:
    """Total size of the file, from Content-Length (or the range total on a resume)."""
    content_length = int(response.headers.get("Content-Length", 0))
    if not content_length:
        return 0
    return content_length + downloaded if resumed else content_length


def download_json(url: str) -> list[dict]:
    response = requests.get(url, timeout=config.DOWNLOAD_TIMEOUT)
    response.raise_for_status()
    return response.json()


def extract_member(zip_path: Path, member: str, dest_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as archive:
        return Path(archive.extract(member, dest_dir))


def single_member(zip_path: Path, suffix: str) -> str:
    with zipfile.ZipFile(zip_path) as archive:
        members = [m for m in archive.namelist() if m.lower().endswith(suffix)]
    if len(members) != 1:
        raise ValueError(f"expected one '{suffix}' member in {zip_path}, got {members}")
    return members[0]


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


def load_zrr(download_dir: Path) -> Extract:
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


def load_geo_api_communes(vintages: Vintages) -> Extract:
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
        load_zrr(download_dir),
        load_frr(vintages, download_dir),
        load_geo_api_communes(vintages),
    ]
    return {extract.table_name: extract for extract in extracts}
