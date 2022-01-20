from scripts.utils import API_KEY, ENDPOINT
import requests


def get_partenaire_culturel(minUpdatedAt=None):
    try:
        headers = {"X-omogen-api-key": API_KEY}

        req = requests.get(
            "{}/partenaire-culturel?dateModificationMin={}".format(
                ENDPOINT, minUpdatedAt
            ),
            headers=headers,
        )
        if req.status_code == 200:
            data = req.json()
            return data
    except Exception as e:
        print("An unexpected error has happened {}".format(e))
    return None


def get_data_adage():
    datas = get_partenaire_culturel("2021-09-01 00:00:00")
    keys = ",".join(list(datas[0].keys()))
    values = ", ".join(
        [
            "({})".format(
                " , ".join(
                    [
                        "'{}'".format(d[k]) if d[k] is not None else "NULL"
                        for k in list(d.keys())
                    ]
                )
            )
            for d in datas
        ]
    )
    return keys, values
