import pandas as pd
from datetime import date, datetime, timedelta
from utils import IOS_APP_ID, ANDROID_APP_ID, TOKEN, save_to_bq
from appsflyer import AppsFlyer
from mapping import (
    DAILY_REPORT,
    DAILY_REPORT_MAPPING,
    INSTALLS_REPORT_MAPPING,
    INSTALLS_REPORT,
    APP_REPORT,
    APP_REPORT_MAPPING,
)

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
        df = pd.concat(dfs, ignore_index=True)
        df = df.rename(columns=INSTALLS_REPORT)
        for k, v in INSTALLS_REPORT_MAPPING.items():
            df[k] = df[k].astype(v)
        return df[list(INSTALLS_REPORT.values()) + ["app"]]

    def get_daily_report(self):
        dfs = []
        for app, api in self.apis.items():
            df = api.daily_report(self._from, self._to, True)
            df["app"] = app
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        df = df.rename(columns=DAILY_REPORT)
        for k, v in DAILY_REPORT_MAPPING.items():
            df[k] = df[k].astype(v)
        return df[list(DAILY_REPORT.values()) + ["app"]]

    def get_in_app_events_report(self):
        dfs = []
        for app, api in self.apis.items():
            df = api.in_app_events_report(self._from, self._to, True)
            df["app"] = app
            dfs.append(df)
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


def run(request):
    _default = default_date()
    try:
        n_days = request.get_json().get("n_days", None)
        table_names = [request.get_json().get("table_name", None)]
    except:
        table_names = ["activity_report", "daily_report", "in_app_event_report"]
        n_days = 5

    try:
        end_date = request.get_json().get("execution_date", None)
        if end_date is None:

            end_date = _default.strftime("%Y-%m-%d")
            start_date = date_minus_n_days(_default, n_days).strftime("%Y-%m-%d")
        else:
            start_date = date_minus_n_days(
                datetime.strptime(end_date, "%Y-%m-%d"), n_days
            ).strftime("%Y-%m-%d")

    except:
        end_date = _default.strftime("%Y-%m-%d")
        start_date = date_minus_n_days(_default, n_days).strftime("%Y-%m-%d")

    import_app = ImportAppsFlyer(start_date, end_date)
    if "activity_report" in table_names:
        print("Run activity_report...")
        save_to_bq(
            import_app.get_install_report(),
            "appsflyer_activity_report",
            start_date,
            end_date,
            INSTALLS_REPORT_MAPPING,
            date_column="event_time",
        )
    if "daily_report" in table_names:
        print("Run daily_report...")
        save_to_bq(
            import_app.get_daily_report(),
            "appsflyer_daily_report",
            start_date,
            end_date,
            DAILY_REPORT_MAPPING,
            date_column="date",
        )
    if "in_app_event_report" in table_names:
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
