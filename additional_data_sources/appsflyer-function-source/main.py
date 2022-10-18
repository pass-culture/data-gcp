import pandas as pd
from datetime import date, timedelta
from utils import IOS_APP_ID, ANDROID_APP_ID, TOKEN, save_to_bq
from appsflyer import AppsFlyer
from mapping import (
    DAILY_REPORT,
    DAILY_REPORT_TYPE,
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

    def get_activity_report(self):
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
        for k, v in DAILY_REPORT_TYPE.items():
            df[k] = df[k].astype(v)
        return df[list(DAILY_REPORT.values()) + ["app"]]

    def get_in_app_events_report(self):
        dfs = []
        for app, api in self.apis.items():
            df = api.in_app_events_report(self._from, self._to, True)
            df["app"] = app
            dfs.append(df)
        df = df.rename(columns=APP_REPORT)
        for k, v in APP_REPORT_MAPPING.items():
            df[k] = df[k].astype(v)
        return df[list(APP_REPORT.values()) + ["app"]]


def default_date():
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def run(request):
    try:
        execution_date = request.get_json().get("execution_date", None)
        if execution_date is None:
            execution_date = default_date()

    except:
        execution_date = default_date()

    import_app = ImportAppsFlyer(execution_date, execution_date)
    save_to_bq(
        import_app.get_activity_report(), "appsflyer_activity_report", execution_date
    )
    save_to_bq(import_app.get_daily_report(), "appsflyer_daily_report", execution_date)
    save_to_bq(
        import_app.get_in_app_events_report(),
        "appsflyer_in_app_event_report",
        execution_date,
    )

    return "Success"
