import logging
import time
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
import typer
from tqdm import tqdm

from src.constants import (
    WIKIDATA_IMAGE_AUTHOR_KEY,
    WIKIDATA_IMAGE_FILE_URL_KEY,
    WIKIDATA_IMAGE_LICENSE_KEY,
    WIKIDATA_IMAGE_LICENSE_URL_KEY,
    WIKIMEDIA_REQUEST_HEADER,
)

logging.basicConfig(level=logging.INFO)
app = typer.Typer()

NO_AUTHOR_VALUE = "Auteur inconnu"
NO_LICENSE_URL_VALUE = "URL de la licence inconnue"
NO_LICENSE_VALUE = "Licence inconnue"
ALLOWED_LICENSES = [
    "CC BY 4.0",
    "CC BY-SA 4.0",
    "CC BY 3.0",
    "CC BY-SA 3.0",
    "Public domain",
    "CC0",
]
LICENSES_COLUMNS = [
    "filename",
    WIKIDATA_IMAGE_FILE_URL_KEY,
    WIKIDATA_IMAGE_AUTHOR_KEY,
    WIKIDATA_IMAGE_LICENSE_KEY,
    WIKIDATA_IMAGE_LICENSE_URL_KEY,
]


def extract_title_from_url(url: str) -> str:
    path = urlparse(url).path
    title = path.split("/")[-1]
    return f"File:{unquote(title)}"


def build_title_to_url_mapping(image_urls: list[str]) -> dict:
    return {extract_title_from_url(url): url for url in image_urls}


def build_wikimedia_query_params(image_urls_per_batch: str) -> dict:
    titles_list = [extract_title_from_url(url) for url in image_urls_per_batch]
    titles = "|".join(titles_list)

    return {
        "action": "query",
        "titles": titles,
        "prop": "imageinfo",
        "iiprop": "extmetadata",
        "format": "json",
    }


def get_image_license(image_urls: list[str]) -> pd.DataFrame:
    WIKIMEDIA_URL = "https://commons.wikimedia.org/w/api.php"
    BATCH_SIZE = 50  # Probably the biggest allowed by wikimedia
    title_to_url_mapping = build_title_to_url_mapping(image_urls)

    # Process images in batches to speedup
    licenses_list = []
    for i in tqdm(range(0, len(image_urls), BATCH_SIZE)):
        image_urls_per_batch = image_urls[i : i + BATCH_SIZE]

        response = requests.get(
            WIKIMEDIA_URL,
            params=build_wikimedia_query_params(
                image_urls_per_batch=image_urls_per_batch
            ),
            headers=WIKIMEDIA_REQUEST_HEADER,
        )

        if response.status_code == 200:
            data = response.json()
            for page in data["query"]["pages"].values():
                image_url = title_to_url_mapping[page["title"]]
                base_response = {
                    "filename": page["title"],
                    WIKIDATA_IMAGE_FILE_URL_KEY: image_url,
                }

                if "imageinfo" in page:
                    extmetadata = page["imageinfo"][0].get("extmetadata", {})
                    license_info = extmetadata.get("LicenseShortName", {}).get(
                        "value", NO_LICENSE_VALUE
                    )
                    author = extmetadata.get("Artist", {}).get("value", NO_AUTHOR_VALUE)
                    license_url = extmetadata.get("LicenseUrl", {}).get(
                        "value", NO_LICENSE_URL_VALUE
                    )
                    license_metadata = {
                        WIKIDATA_IMAGE_AUTHOR_KEY: author,
                        WIKIDATA_IMAGE_LICENSE_KEY: license_info,
                        WIKIDATA_IMAGE_LICENSE_URL_KEY: license_url,
                    }

                else:
                    license_metadata = {
                        WIKIDATA_IMAGE_AUTHOR_KEY: NO_AUTHOR_VALUE,
                        WIKIDATA_IMAGE_LICENSE_KEY: NO_LICENSE_VALUE,
                        WIKIDATA_IMAGE_LICENSE_URL_KEY: NO_LICENSE_URL_VALUE,
                    }

                licenses_list.append({**base_response, **license_metadata})
        else:
            raise ValueError(
                f"Failed to fetch data for batch starting with {image_urls_per_batch[0]}"
            )

        # Sleep to avoid hitting the API rate limits
        time.sleep(0.5)

    return pd.DataFrame(
        licenses_list,
        columns=LICENSES_COLUMNS,
    )


def remove_image_with_improper_license(df: pd.DataFrame) -> pd.DataFrame:
    indexes_to_remove_images = df[WIKIDATA_IMAGE_LICENSE_KEY].notna() & ~df[
        WIKIDATA_IMAGE_LICENSE_KEY
    ].isin(ALLOWED_LICENSES)
    df.loc[
        indexes_to_remove_images,
        LICENSES_COLUMNS,
    ] = None

    return df


@app.command()
def main(
    artists_matched_on_wikidata: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artists_df = pd.read_parquet(artists_matched_on_wikidata).rename(
        columns={"img": WIKIDATA_IMAGE_FILE_URL_KEY}
    )

    # Fetch the licenses from wikidata
    image_list = (
        artists_df[WIKIDATA_IMAGE_FILE_URL_KEY].dropna().drop_duplicates().tolist()
    )
    image_license_df = get_image_license(image_list)

    artists_with_licenses_df = artists_df.merge(
        image_license_df, how="left", on=WIKIDATA_IMAGE_FILE_URL_KEY
    ).pipe(remove_image_with_improper_license)

    artists_with_licenses_df.to_parquet(output_file_path)


if __name__ == "__main__":
    app()
