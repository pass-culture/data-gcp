import typer
from pytiktok import BusinessAccountApi

from extract import account_import, videos_import
from utils import CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN


def main(
    start_date: str = typer.Option(
        ...,
        help="Start date for exporting accounts data.",
    ),
    end_date: str = typer.Option(
        ...,
        help="End date for exporting accounts data.",
    ),
):
    print(CLIENT_ID)
    print(CLIENT_SECRET)
    print(REFRESH_TOKEN)
    business_api = BusinessAccountApi(
        app_id=CLIENT_ID,
        app_secret=CLIENT_SECRET,
    )
    json_keys = business_api.refresh_access_token(REFRESH_TOKEN, return_json=True)
    account_username = account_import(
        business_api,
        business_id=json_keys["creator_id"],
        from_date=start_date,
        to_date=end_date,
    )
    videos_import(
        business_api,
        business_id=json_keys["creator_id"],
        account_username=account_username,
        export_date=end_date,
    )


if __name__ == "__main__":
    typer.run(main)
