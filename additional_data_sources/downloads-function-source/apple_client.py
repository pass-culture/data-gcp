import requests
import time

import numpy as np
import pandas as pd
import zlib
from authlib.jose import jwt


class AppleClient:
    def __init__(self, key_id, issuer_id, private_key):
        self.key_id = key_id
        self.issuer_id = issuer_id
        self.private_key = private_key

    def get_monthly_donloads(self, month="2021-05"):
        expiration_time = int(
            round(time.time() + (20.0 * 60.0))
        )  # 20 minutes timestamp
        header = {"alg": "ES256", "kid": self.key_id, "typ": "JWT"}

        payload = {
            "iss": self.issuer_id,
            "exp": expiration_time,
            "aud": "appstoreconnect-v1",
        }

        # Create the JWT
        token = jwt.encode(header, payload, self.private_key)

        # API Request
        jwt_bearer = "Bearer " + token.decode()
        url = "https://api.appstoreconnect.apple.com/v1/salesReports"
        head = {"Authorization": jwt_bearer}

        r = requests.get(
            url,
            params={
                "filter[frequency]": "MONTHLY",
                "filter[reportDate]": month,
                "filter[reportSubType]": "SUMMARY",
                "filter[reportType]": "SALES",
                "filter[vendorNumber]": "89881612",
            },
            headers=head,
        )
        print(r.status_code)

        data = zlib.decompress(r.content, zlib.MAX_WBITS | 32)

        with open("/tmp/report.txt", "wb") as outFile:
            outFile.write(data)

        with open("/tmp/report.txt", "r") as f:
            data = f.read()

        df = pd.DataFrame([x.rsplit("\t") for x in data.rsplit("\n")[1:]])
        df.columns = [
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
        return np.sum([int(x) for x in df["Units"].values if x is not None])
