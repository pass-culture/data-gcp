import time
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils import get_utm_campaign

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


class BatchClient:
    def __init__(self, api_key, rest_api_key, operating_system):
        print(api_key, rest_api_key)
        self.api_key = api_key
        self.rest_api_key = rest_api_key
        self.headers = {
            "Content-Type": "application/json",
            "X-Authorization": f"{self.rest_api_key}",
        }
        self.base_url = "https://api.batch.com/1.1/"
        self.operating_system = operating_system

    def get_campaigns_metadata(self):
        """Returns a dataframe with the list of Batch campaigns with their metadata."""

        offset = 0
        url = f"{self.base_url}{self.api_key}/campaigns/list?limit=100&from={offset}"
        api_responses = []
        print("Request...")
        response = session.get(url, headers=self.headers)
        api_responses.append(response.json())
        print("Loop...")
        while response is not None and len(response.json()) == 100:
            offset = offset + 100
            url = (
                f"{self.base_url}{self.api_key}/campaigns/list?limit=100&from={offset}"
            )
            response = session.get(url, headers=self.headers)
            api_responses.append(response.json())

        campaigns_list = []
        for response in api_responses:
            for campaign in response:
                campaigns_list.append(campaign)

        campaigns_df = pd.DataFrame.from_records(campaigns_list).assign(
            created_date=lambda _df: pd.to_datetime(_df["created_date"]),
            operating_system=self.operating_system,
        )[["campaign_token", "dev_only", "created_date", "name", "live", "from_api"]]

        # add tags
        tags_list = []
        for campaign in campaigns_df["campaign_token"].unique():
            url = f"{self.base_url}{self.api_key}/campaigns/{campaign}"
            response = session.get(url, headers=self.headers)
            if response.status_code == 200:
                tags_dict = {
                    key: response.json().get(key, [])
                    for key in ["campaign_token", "labels"]
                }
                tags_dict["messages"] = response.json().get("messages", [{}])

                if isinstance(tags_dict["messages"], list):
                    tags_dict["deeplink"] = tags_dict["messages"][0].get("deeplink", "")
                    tags_dict["utm_campaign"] = get_utm_campaign(tags_dict["deeplink"])

                if isinstance(tags_dict["messages"], dict):
                    if "b" in tags_dict["messages"]:
                        tags_dict["deeplink"] = {
                            "a": tags_dict["messages"]["a"][0].get("deeplink", ""),
                            "b": tags_dict["messages"]["b"][0].get("deeplink", ""),
                        }
                        tags_dict["utm_campaign"] = {
                            "a": get_utm_campaign(tags_dict["deeplink"]["a"]),
                            "b": get_utm_campaign(tags_dict["deeplink"]["b"]),
                        }

                    else:
                        tags_dict["deeplink"] = {
                            "a": tags_dict["messages"]["a"][0].get("deeplink", "")
                        }
                        tags_dict["utm_campaign"] = {
                            "a": get_utm_campaign(tags_dict["deeplink"]["a"])
                        }
                tags_dict.pop("deeplink", None)
                tags_dict.pop("messages", None)
                tags_list.append(tags_dict)

        campaigns_with_tag_df = (
            pd.DataFrame(tags_list)
            .rename(columns={"labels": "tags"})
            .assign(tags=lambda _df: _df["tags"].astype(str))
        )

        if campaigns_with_tag_df["utm_campaign"].apply(pd.Series).shape[1] == 3:
            campaigns_with_tag_df[
                ["utm_campaign", "utm_campaign_a", "utm_campaign_b"]
            ] = campaigns_with_tag_df["utm_campaign"].apply(pd.Series)

            campaigns_with_tag_df = (
                campaigns_with_tag_df.assign(
                    utm_campaign=lambda _df: _df["utm_campaign"].fillna(
                        _df["utm_campaign_a"]
                    )
                )
                .drop(["utm_campaign_a"], axis=1)
                .rename(columns={"utm_campaign_b": "utm_campaign_version_b"})
            )

        campaigns_df = campaigns_df.merge(
            campaigns_with_tag_df, on="campaign_token", how="left"
        )

        return campaigns_df

    def get_campaigns_stats(self):
        """Returns a dataframe with the statistics of performance of Batch campaigns."""

        dfs_list = []
        i = 0
        url = f"{self.base_url}{self.api_key}/campaigns/stats"
        # Get live campaigns tokens
        campaigns_metadata_df = self.get_campaigns_metadata()
        live_campaigns_token = campaigns_metadata_df.query("live == True")[
            "campaign_token"
        ].to_list()

        for campaign_token in live_campaigns_token:
            print(
                f"Retrieving stats of the {i}th campaign. Campaign token : {campaign_token}"
            )
            response = session.get(f"{url}/{campaign_token}", headers=self.headers)
            i += 1
            if response.status_code == 200 and len(response.json()["detail"]) > 0:
                response_df = pd.DataFrame.from_records(response.json()["detail"])
                response_df["campaign_token"] = response.json()["campaign_token"]
                dfs_list.append(response_df)
            if i % 60 == 0:  # limit at 60 calls each minute = 5 sec by requests
                print(f"Datetime: {datetime.now()}")
                print("wait 1 min")
                time.sleep(60)

        campaigns_stats_df = pd.concat(dfs_list).assign(
            update_date=pd.to_datetime("today")
        )

        return campaigns_stats_df

    def get_ab_testing_details(self, campaigns_stats_df):
        ab_testing_df = (
            campaigns_stats_df[~campaigns_stats_df["versions"].isna()]
            .reset_index(drop=True)
            .assign(
                version_a=lambda _df: _df["versions"].apply(
                    lambda x: x.get("a", None) if type(x) is dict else x
                ),
                version_b=lambda _df: _df["versions"].apply(
                    lambda x: x.get("b", None) if type(x) is dict else x
                ),
            )
            .drop("versions", axis=1)
        )

        version_a_list = [data for data in ab_testing_df["version_a"]]
        i = 0
        for d in version_a_list:
            d["campaign_token"] = ab_testing_df["campaign_token"][i]
            d["date"] = ab_testing_df["date"][i]
            d["errors"] = ab_testing_df["errors"][i]
            i += 1
        version_a_df = pd.DataFrame.from_records(version_a_list)
        version_a_df["version"] = "a"

        version_b_list = [data for data in ab_testing_df["version_b"]]
        j = 0
        for d in version_b_list:
            d["campaign_token"] = ab_testing_df["campaign_token"][j]
            d["date"] = ab_testing_df["date"][j]
            d["errors"] = ab_testing_df["errors"][j]
            j += 1
        version_b_df = pd.DataFrame.from_records(version_b_list)
        version_b_df["version"] = "b"

        ab_testing_df = pd.concat([version_a_df, version_b_df]).reset_index(drop=True)

        return ab_testing_df

    def get_campaigns_stats_detailed(self, campaigns_stats_df, ab_testing_df):
        final_df = pd.concat(
            [campaigns_stats_df[campaigns_stats_df["versions"].isna()], ab_testing_df]
        ).drop("versions", axis=1)

        return final_df

    def get_transactional_stats(self, group_id, start_date, end_date):
        url = f"{self.base_url}{self.api_key}/transactional/stats"
        response = session.get(
            f"{url}/{group_id}?since={start_date}&until={end_date}",
            headers=self.headers,
        )
        print("response: ", response.json())
        stats_df = pd.DataFrame.from_records(response.json()["detail"]).assign(
            group_id=group_id, update_date=pd.to_datetime("today")
        )

        return stats_df
