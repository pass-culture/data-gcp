import logging
import zipfile
from pathlib import Path

import requests

from utils.config import DOWNLOAD_TIMEOUT

logger = logging.getLogger(__name__)


def download(url: str, dest: Path) -> Path:
    """Stream `url` to `dest`, reusing an existing file (the sources are yearly snapshots)."""
    if dest.exists() and dest.stat().st_size > 0:
        logger.info("Reusing %s", dest)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as response:
        response.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with tmp.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1 << 20):
                f.write(chunk)
        tmp.replace(dest)
    return dest


def download_json(url: str) -> list[dict]:
    response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
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
