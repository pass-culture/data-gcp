import requests
import time

import pandas as pd
import zlib
from authlib.jose import jwt

OUT_COLS = [
    "provider",
    "provider_country",
    "sku",
    "developer",
    "title",
    "version",
    "product_type_identifier",
    "units",
    "developer_proceeds",
    "begin_date",
    "end_date",
    "customer_currency",
    "country_code",
    "currency_of_proceeds",
    "apple_identifier",
    "customer_price",
    "promo_code",
    "parent_identifier",
    "subscription",
    "period",
    "category",
    "cmb",
    "device",
    "supported_platforms",
    "proceeds_reason",
    "preserved_pricing",
    "client",
    "order_type",
]


class AppleClient:
    def __init__(self, key_id, issuer_id, private_key):
        expiration_time = int(
            round(time.time() + (20.0 * 60.0))
        )  # 20 minutes timestamp
        header = {"alg": "ES256", "kid": key_id, "typ": "JWT"}

        payload = {
            "iss": issuer_id,
            "exp": expiration_time,
            "aud": "appstoreconnect-v1",
        }

        # Create the JWT
        token = jwt.encode(header, payload, private_key)

        jwt_bearer = "Bearer " + token.decode()
        self.url = "https://api.appstoreconnect.apple.com/v1/salesReports"
        self.head = {"Authorization": jwt_bearer}

    def get_downloads(self, frequency="MONTHLY", report_date="2021-05"):
        r = requests.get(
            self.url,
            params={
                "filter[frequency]": frequency,
                "filter[reportDate]": report_date,
                "filter[reportSubType]": "SUMMARY",
                "filter[reportType]": "SALES",
                "filter[vendorNumber]": "89881612",
            },
            headers=self.head,
        )

        if r.status_code == 404:
            return None
        try:
            data = zlib.decompress(r.content, zlib.MAX_WBITS | 32)
        except:
            print(f"Error with {report_date}")
            print(r.status_code)
            print(r.content)
            return None
        with open("/tmp/report.txt", "wb") as outFile:
            outFile.write(data)

        with open("/tmp/report.txt", "r") as f:
            data = f.read()

        df = pd.DataFrame([x.rsplit("\t") for x in data.rsplit("\n")[1:]])
        df.columns = OUT_COLS
        df["units"] = df["units"].astype(float)
        df = df[df["units"] > 0]
        df["date"] = report_date
        return df
