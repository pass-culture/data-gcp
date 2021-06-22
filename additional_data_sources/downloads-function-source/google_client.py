import pandas as pd


class GoogleClient:
    def __init__(self, report_bucket_name):
        self.report_bucket_name = report_bucket_name

    def get_monthly_donloads(self, month="2021-05"):
        file_name = f"stats/installs/installs_app.passculture.webapp_{month.replace('-', '')}_app_version.csv"
        path = f"gs://{self.report_bucket_name}/{file_name}"
        df = pd.read_csv(path, encoding="utf-16")
        return df["Install events"].sum()
