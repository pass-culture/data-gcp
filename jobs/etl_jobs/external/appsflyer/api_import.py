import time
from datetime import date, timedelta

import pandas as pd
import typer

from core.appsflyer import AppsFlyer
from core.mapping import (
    APP_REPORT,
    APP_REPORT_MAPPING,
    DAILY_REPORT,
    DAILY_REPORT_MAPPING,
    INSTALLS_REPORT,
    INSTALLS_REPORT_MAPPING,
    PARTNER_REPORT,
    PARTNER_REPORT_MAPPING,
)
from core.utils import ANDROID_APP_ID, IOS_APP_ID, TOKEN, save_to_bq

APPS = {"ios": IOS_APP_ID, "android": ANDROID_APP_ID}


class ImportAppsFlyer:
    def __init__(self, _from, _to):
        self._from = _from
        self._to = _to
        self.apis = {app: AppsFlyer(TOKEN, _id) for app, _id in APPS.items()}

    def get_install_report(self):
        dfs = []
        for app, api in self.apis.items():
            df_installs = api.installs_report(self._from, self._to, True)
            df_uninstalls = api.uninstall_events_report(self._from, self._to, True)
            df_installs["app"] = app
            df_uninstalls["app"] = app
            dfs.append(df_installs)
            dfs.append(df_uninstalls)
            time.sleep(60)
        df = pd.concat(dfs, ignore_index=True)
        df = df.rename(columns=INSTALLS_REPORT)
        for k, v in INSTALLS_REPORT_MAPPING.items():
            df[k] = df[k].astype(v)
        return df[list(INSTALLS_REPORT.values()) + ["app"]]

    def get_daily_report(self):
        dfs = []
        for app, api in self.apis.items():
            # Facebook

            df = api.daily_report(self._from, self._to, True, category="facebook")
            _cols = list(df.columns)
            df["app"] = app
            if "Adset Id" in _cols:
                df["Adset Id"] = df["Adset Id"].map(lambda x: "{:.0f}".format(x))
            if "Adgroup Id" in _cols:
                df["Adgroup Id"] = df["Adgroup Id"].map(lambda x: "{:.0f}".format(x))
            if "Campaign Id" in _cols:
                df["Campaign Id"] = df["Campaign Id"].map(lambda x: "{:.0f}".format(x))
            dfs.append(df)
            time.sleep(60)
            # Else
            df = api.daily_report(self._from, self._to, True, category="standard")
            df = df[df["Media Source (pid)"] != "Facebook Ads"]

            df["app"] = app
            dfs.append(df)
            time.sleep(60)
        df = pd.concat(dfs, ignore_index=True)
        df = df.rename(columns=DAILY_REPORT)
        df_columns = list(df.columns)
        for k, v in DAILY_REPORT_MAPPING.items():
            if k not in df_columns:
                df[k] = None
            df[k] = df[k].astype(v)
        return df[list(DAILY_REPORT.values()) + ["app"]]

    def get_partner_report(self):
        dfs = []
        for app, api in self.apis.items():
            # Facebook

            df = api.partners_by_date_report(
                self._from, self._to, True, category="facebook"
            )
            _cols = list(df.columns)
            df["app"] = app
            if "Adset Id" in _cols:
                df["Adset Id"] = df["Adset Id"].map(lambda x: "{:.0f}".format(x))
            if "Adgroup Id" in _cols:
                df["Adgroup Id"] = df["Adgroup Id"].map(lambda x: "{:.0f}".format(x))
            if "Campaign Id" in _cols:
                df["Campaign Id"] = df["Campaign Id"].map(lambda x: "{:.0f}".format(x))
            dfs.append(df)
            time.sleep(60)
            # Else
            df = api.partners_by_date_report(
                self._from, self._to, True, category="standard"
            )
            df = df[df["Media Source (pid)"] != "Facebook Ads"]

            df["app"] = app
            dfs.append(df)
            time.sleep(60)
        df = pd.concat(dfs, ignore_index=True)
        df = df.rename(columns=PARTNER_REPORT)
        df_columns = list(df.columns)
        for k, v in PARTNER_REPORT_MAPPING.items():
            if k not in df_columns:
                df[k] = None
            df[k] = df[k].astype(v)
        return df[list(PARTNER_REPORT.values()) + ["app"]]

    def get_in_app_events_report(self):
        dfs = []
        for app, api in self.apis.items():
            df = api.in_app_events_report(
                self._from, self._to, True, maximum_rows=1000000
            )
            df["app"] = app
            dfs.append(df)
            time.sleep(60)
        df = pd.concat(dfs, ignore_index=True)
        df = df.rename(columns=APP_REPORT)
        for k, v in APP_REPORT_MAPPING.items():
            if k not in df.columns:
                df[k] = None
            df[k] = df[k].astype(v)

        return df[list(APP_REPORT.values()) + ["app"]]


def default_date():
    return date.today() - timedelta(days=1)


def date_minus_n_days(current_date, n_days):
    return current_date - timedelta(days=n_days)


def run(
    n_days: int = typer.Option(
        None,
        help="Nombre de jours",
    ),
    table_name: str = typer.Option(
        ...,
        help="Nom de la table à importer",
    ),
    end_date: str = typer.Option(
        None,
        help="Date de fin",
    ),
    start_date: str = typer.Option(
        None,
        help="Date de début",
    ),
):
    if n_days is not None:
        _default = default_date()
        end_date = _default.strftime("%Y-%m-%d")

        start_date = date_minus_n_days(_default, n_days).strftime("%Y-%m-%d")
    elif start_date is None or end_date is None:
        raise Exception("n_days or start_date | end_date should be not None")

    import_app = ImportAppsFlyer(start_date, end_date)
    if "activity_report" == table_name:
        print("Run activity_report...")
        save_to_bq(
            import_app.get_install_report(),
            "appsflyer_activity_report",
            start_date,
            end_date,
            INSTALLS_REPORT_MAPPING,
            date_column="event_time",
        )
    if "daily_report" == table_name:
        print("Run daily_report...")
        save_to_bq(
            import_app.get_daily_report(),
            "appsflyer_daily_report",
            start_date,
            end_date,
            DAILY_REPORT_MAPPING,
            date_column="date",
        )
    if "partner_report" == table_name:
        print("Run partner_report...")
        save_to_bq(
            import_app.get_partner_report(),
            "appsflyer_partner_report",
            start_date,
            end_date,
            DAILY_REPORT_MAPPING,
            date_column="date",
        )
    if "in_app_event_report" == table_name:
        print("Run in_app_event_report...")
        save_to_bq(
            import_app.get_in_app_events_report(),
            "appsflyer_in_app_event_report",
            start_date,
            end_date,
            APP_REPORT_MAPPING,
            date_column="event_time",
        )

    return "Success"


if __name__ == "__main__":
    typer.run(run)
