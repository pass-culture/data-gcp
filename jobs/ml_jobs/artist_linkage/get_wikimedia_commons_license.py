import logging
import time
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
import typer
from tqdm import tqdm

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
    "image_file_url",
    "image_page_url",
    "image_author",
    "image_license",
    "image_license_url",
]


def extract_title_from_url(url: str) -> str:
    path = urlparse(url).path
    title = path.split("/")[-1]
    return f"File:{unquote(title)}"


def build_title_to_url_mapping(image_urls: list[str]) -> dict:
    return {extract_title_from_url(url): url for url in image_urls}


def extract_page_url_from_title(title: str) -> str:
    return f"https://commons.wikimedia.org/wiki/{title}"


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
        )

        if response.status_code == 200:
            data = response.json()
            for page in data["query"]["pages"].values():
                image_url = title_to_url_mapping[page["title"]]
                page_url = extract_page_url_from_title(title=page["title"])
                base_response = {
                    "filename": page["title"],
                    "image_file_url": image_url,
                    "image_page_url": page_url,
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
                        "image_author": author,
                        "image_license": license_info,
                        "image_license_url": license_url,
                    }

                else:
                    license_metadata = {
                        "image_author": NO_AUTHOR_VALUE,
                        "image_license": NO_LICENSE_VALUE,
                        "image_license_url": NO_LICENSE_URL_VALUE,
                    }

                licenses_list.append({**base_response, **license_metadata})
        else:
            logging.warning(
                f"Failed to fetch data for batch starting with {image_urls_per_batch[0]}"
            )

        # Sleep to avoid hitting the API rate limits
        time.sleep(0.5)

    return pd.DataFrame(
        licenses_list,
        columns=LICENSES_COLUMNS,
    )


def remove_image_with_improper_license(df: pd.DataFrame) -> pd.DataFrame:
    indexes_to_remove_images = df.image_license.notna() & ~df.image_license.isin(
        ALLOWED_LICENSES
    )
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
        columns={"img": "image_file_url"}
    )

    # Fetch the licenses from wikidata
    image_list = artists_df.image_file_url.dropna().drop_duplicates().tolist()
    image_license_df = get_image_license(image_list)

    artists_with_licenses_df = artists_df.merge(
        image_license_df, how="left", on="image_file_url"
    ).pipe(remove_image_with_improper_license)

    artists_with_licenses_df.to_parquet(output_file_path)


if __name__ == "__main__":
    app()
