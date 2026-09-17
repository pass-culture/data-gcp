INSEE_FILE_BASE_URL = "https://www.insee.fr/fr/statistiques/fichier"

# INSEE publishes each COG vintage under a new page id.
COG_PAGE_IDS = {
    2024: 7766585,
    2025: 8377162,
    2026: 8740222,
}

CONTOUR_IRIS_BASE_URL = "https://data.geopf.fr/telechargement/download/CONTOURS-IRIS"

ZRR_URL = (
    "https://static.data.gouv.fr/resources/zones-de-revitalisation-rurale-zrr/"
    "20210907-124104/diffusion-zonages-zrr-cog2021.xls"
)
ZRR_SHEET = "Classement ZRR (COG 2021)"

FRR_BASE_URL = (
    "https://www.observatoire-des-territoires.gouv.fr/outils/cartographie-interactive"
    "/api/v1/functions/GC_API_download.php"
)
FRR_SHEET = "Data"

GEO_API_COMMUNES_URL = "https://geo.api.gouv.fr/communes"
# Overseas collectivities without IRIS coverage in IGN Contours IRIS.
GEO_API_DEPARTMENTS = ["986", "987", "988"]

DOWNLOAD_TIMEOUT = 300


def cog_url(year: int) -> str:
    return f"{INSEE_FILE_BASE_URL}/{COG_PAGE_IDS[year]}/cog_ensemble_{year}_csv.zip"


def epci_url(year: int) -> str:
    return f"{INSEE_FILE_BASE_URL}/2510634/epci_au_01-01-{year}.zip"


def density_grid_url(year: int) -> str:
    return f"{INSEE_FILE_BASE_URL}/6439600/grille_densite_7_niveaux_{year}.xlsx"


def contour_iris_url(year: int) -> str:
    edition = f"CONTOURS-IRIS_3-0__GEOPARQUET_WGS84G_FRA_{year}-01-01"
    return f"{CONTOUR_IRIS_BASE_URL}/{edition}/contours_iris.parquet"


def frr_url(year: int) -> str:
    return f"{FRR_BASE_URL}?type=stat&nivgeo=com{year}&dataset=frr&indic=codefrr"


def geo_api_communes_url(department_code: str) -> str:
    return (
        f"{GEO_API_COMMUNES_URL}?codeDepartement={department_code}"
        "&fields=code,nom,codeDepartement,codeRegion,contour&format=json"
    )
