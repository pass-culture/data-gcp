import requests
import time

import numpy as np
import pandas as pd
import zlib
from authlib.jose import jwt

OUT_COLS = [
    "Provider",
    "Provider Country",
    "SKU",
    "Developer",
    "Title",
    "Version",
    "Product Type Identifier",
    "Units",
    "Developer Proceeds",
    "Begin Date",
    "End Date",
    "Customer Currency",
    "Country Code",
    "Currency of Proceeds",
    "Apple Identifier",
    "Customer Price",
    "Promo Code",
    "Parent Identifier",
    "Subscription",
    "Period",
    "Category",
    "CMB",
    "Device",
    "Supported Platforms",
    "Proceeds Reason",
    "Preserved Pricing",
    "Client",
    "Order Type",
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
        print(r.status_code)
        if r.status_code == 404:
            return 0

        data = zlib.decompress(r.content, zlib.MAX_WBITS | 32)

        with open("/tmp/report.txt", "wb") as outFile:
            outFile.write(data)

        with open("/tmp/report.txt", "r") as f:
            data = f.read()

        df = pd.DataFrame([x.rsplit("\t") for x in data.rsplit("\n")[1:]])
        df.columns = OUT_COLS
        df['date'] = report_date
        return df
