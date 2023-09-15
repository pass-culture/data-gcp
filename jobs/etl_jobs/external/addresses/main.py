from datetime import datetime
from scripts import fetch_user_location

STORAGE_PATH = "addresses_exports"


def run():
    """The Cloud Function entrypoint."""

    now = datetime.now().isoformat(timespec="minutes")
    user_locations_file_name = f"{STORAGE_PATH}/user_locations_{now}.csv"

    downloader = fetch_user_location.AdressesDownloader(user_locations_file_name)
    result = downloader.run()

    return result


if __name__ == "__main__":
    run()
