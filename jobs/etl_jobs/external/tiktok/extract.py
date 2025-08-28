import time

import pandas as pd

from utils import (
    TIKTOK_ACCOUNT_DAILY_ACTIVITY,
    TIKTOK_ACCOUNT_HOURLY_AUDIENCE,
    TIKTOK_VIDEO_AUDIENCE_COUNTRY,
    TIKTOK_VIDEO_DETAIL,
    TIKTOK_VIDEO_IMPRESSION_SOURCE,
    save_to_bq,
)


def extract_hourly_activity_data(json_data: list[dict]) -> pd.DataFrame:
    hourly_activity_data = []
    for entry in json_data:
        date = entry["date"]
        for activity in entry.get("audience_activity", []):
            hourly_activity_data.append(
                {"date": date, "hour": activity["hour"], "audience": activity["count"]}
            )
    return pd.DataFrame(hourly_activity_data)


def create_daily_activity_df(json_data: list[dict]) -> pd.DataFrame:
    daily_activity_data = [
        {
            "date": entry["date"],
            "video_views": entry["video_views"],
            "likes": entry["likes"],
            "followers_count": entry["followers_count"],
            "shares": entry["shares"],
            "profile_views": entry["profile_views"],
            "comments": entry["comments"],
        }
        for entry in json_data
    ]
    return pd.DataFrame(daily_activity_data)


def extract_impression_sources_data(videos: list[dict]) -> pd.DataFrame:
    impression_sources_data = []
    for video in videos:
        item_id = video["item_id"]
        for source in video["impression_sources"]:
            impression_sources_data.append(
                {
                    "item_id": item_id,
                    "impression_source": source["impression_source"],
                    "percentage": source["percentage"],
                }
            )
    return pd.DataFrame(impression_sources_data)


def extract_audience_countries_data(videos: list[dict]) -> pd.DataFrame:
    audience_countries_data = []
    for video in videos:
        item_id = video["item_id"]
        for country in video["audience_countries"]:
            audience_countries_data.append(
                {
                    "item_id": item_id,
                    "country": country["country"],
                    "percentage": country["percentage"],
                }
            )
    return pd.DataFrame(audience_countries_data)


def create_videos_df(videos: list[dict]) -> pd.DataFrame:
    videos_data = [
        {
            "item_id": video["item_id"],
            "caption": video["caption"],
            "average_time_watched": video["average_time_watched"],
            "reach": video["reach"],
            "create_time": video["create_time"],
            "share_url": video["share_url"],
            "video_views": video["video_views"],
            "total_time_watched": video["total_time_watched"],
            "likes": video["likes"],
            "shares": video["shares"],
            "comments": video["comments"],
            "video_duration": video["video_duration"],
            "full_video_watched_rate": video["full_video_watched_rate"],
            "thumbnail_url": video["thumbnail_url"],
            "embed_url": video["embed_url"],
        }
        for video in videos
    ]
    return pd.DataFrame(videos_data)


def extract_videos(business_api, business_id: str):
    videos_data = []
    has_more = True
    cursor = None
    it = 0
    while has_more:
        results = business_api.get_account_videos(
            business_id=business_id,
            fields=[
                "item_id",
                "create_time",
                "thumbnail_url",
                "caption",
                "share_url",
                "embed_url",
                "video_views",
                "likes",
                "comments",
                "shares",
                "reach",
                "video_duration",
                "full_video_watched_rate",
                "total_time_watched",
                "average_time_watched",
                "impression_sources",
                "audience_countries",
            ],
            cursor=cursor,
            max_count=20,
            return_json=True,
        )

        if results["message"] == "OK":
            videos_data.extend(results["data"]["videos"])
            cursor = results["data"]["cursor"]
            has_more = results["data"]["has_more"]
        else:
            has_more = False
        it += 1
        print(f"Importing videos {it}/X")
    return videos_data


def videos_import(
    business_api, business_id: str, account_username: str, export_date: str
) -> None:
    videos_data = extract_videos(business_api, business_id)

    impression_sources_data_df = extract_impression_sources_data(videos_data)
    impression_sources_data_df["account"] = str(account_username)
    impression_sources_data_df["export_date"] = export_date
    save_to_bq(
        impression_sources_data_df,
        TIKTOK_VIDEO_IMPRESSION_SOURCE,
        export_date,
        export_date,
        "export_date",
    )

    audience_countries_data_df = extract_audience_countries_data(videos_data)
    audience_countries_data_df["account"] = account_username
    audience_countries_data_df["export_date"] = export_date
    save_to_bq(
        audience_countries_data_df,
        TIKTOK_VIDEO_AUDIENCE_COUNTRY,
        export_date,
        export_date,
        "export_date",
    )

    videos_data_df = create_videos_df(videos_data)
    videos_data_df["account"] = account_username
    videos_data_df["export_date"] = export_date
    save_to_bq(
        videos_data_df, TIKTOK_VIDEO_DETAIL, export_date, export_date, "export_date"
    )


def account_import(business_api, business_id: str, from_date: str, to_date: str) -> str:
    max_retries = 20
    retry_delay = 10

    for attempt in range(max_retries):
        account_stats = business_api.get_account_data(
            business_id=business_id,
            return_json=True,
            start_date=from_date,
            end_date=to_date,
            fields=[
                "username",
                "display_name",
                "profile_image",
                "audience_countries",
                "audience_genders",
                "likes",
                "comments",
                "shares",
                "followers_count",
                "profile_views",
                "video_views",
                "audience_activity",
            ],
        )

        # Check if metrics are available
        if "metrics" in account_stats["data"]:
            print(f"Successfully retrieved metrics on attempt {attempt + 1}")
            break
        else:
            print(f"Metrics not available, attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                print(f"Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)

    # Extracting the audience_activity data
    if "metrics" in account_stats["data"]:
        print(account_stats["data"]["metrics"])
        tiktok_hourly_audience_activity_df = extract_hourly_activity_data(
            json_data=account_stats["data"]["metrics"]
        )
        if tiktok_hourly_audience_activity_df.shape[0] > 0:
            tiktok_hourly_audience_activity_df["account"] = account_stats["data"][
                "username"
            ]
            save_to_bq(
                tiktok_hourly_audience_activity_df,
                TIKTOK_ACCOUNT_HOURLY_AUDIENCE,
                from_date,
                to_date,
                "date",
            )

        # Creating the tiktok_daily_activity dataframe
        tiktok_daily_activity_df = create_daily_activity_df(
            json_data=account_stats["data"]["metrics"]
        )
        tiktok_daily_activity_df["account"] = account_stats["data"]["username"]
        save_to_bq(
            tiktok_daily_activity_df,
            TIKTOK_ACCOUNT_DAILY_ACTIVITY,
            from_date,
            to_date,
            "date",
        )

        return account_stats["data"]["username"]
