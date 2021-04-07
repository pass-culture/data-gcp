import os
from datetime import datetime
from scripts import fetch_user_location


project_name = os.environ["PROJECT_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
STORAGE_PATH = f"{BUCKET_NAME}/addresses_exports/"


def run(request):
    """The Cloud Function entrypoint."""

    now = datetime.now().isoformat(timespec="minutes")
    user_locations_file_name = STORAGE_PATH + f"user_locations_{now}.csv"

    downloader = fetch_user_location.AdressesDownloader(
        project_name, user_locations_file_name
    )
    result = downloader.run()

    return result
