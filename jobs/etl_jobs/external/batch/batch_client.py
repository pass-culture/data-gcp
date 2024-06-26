import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

        print(f"{len(live_campaigns_token)} campaigns to load")

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
                    lambda x: x.get("a", None) if isinstance(x, dict) else x
                ),
                version_b=lambda _df: _df["versions"].apply(
                    lambda x: x.get("b", None) if isinstance(x, dict) else x
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
