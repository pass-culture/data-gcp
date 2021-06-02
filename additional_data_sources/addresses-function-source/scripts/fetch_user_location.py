import csv
import json
import os
import time
from datetime import datetime
from urllib.parse import quote
import requests
import gcsfs
from shapely.geometry import Point, Polygon
from scripts.bigquery_client import BigQueryClient

bigquery_client = BigQueryClient()

GCP_PROJECT = os.environ["PROJECT_NAME"]
BIGQUERY_RAW_DATASET = os.environ["BIGQUERY_RAW_DATASET"]
BIGQUERY_CLEAN_DATASET = os.environ["BIGQUERY_CLEAN_DATASET"]
BUCKET_NAME = os.environ["BUCKET_NAME"]


class AdressesDownloader:
    def __init__(self, user_locations_file_name):
        self.user_locations_file_name = user_locations_file_name
        self.user_address_dataframe = None

    def fetch_new_user_data(self):
        bigquery_query = f"""
        SELECT user_id, REPLACE(REPLACE(user_address, '\\r', ''), '\\n', '') AS user_address, user_postal_code, user_city, user_department_code
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_user`
        JOIN `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_beneficiary_import`ON beneficiaryId = user_id
        WHERE user_address is not NULL AND user_address <> ""
        AND (
            (user_postal_code is not NULL AND user_city is not NULL AND user_department_code is not NULL)
            OR source="demarches_simplifiees"
            )
        AND user_id not in (SELECT user_id FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.user_locations`)
        ORDER BY user_id LIMIT 500;
        """

        user_address_dataframe = bigquery_client.query(bigquery_query)
        return user_address_dataframe

    def add_parsed_adress(self):
        self.user_address_dataframe["parsed_address"] = self.user_address_dataframe[
            ["user_address", "user_postal_code", "user_city"]
        ].apply(
            lambda row: quote(" ".join(filter(None, row))),
            axis=1,
        )

    def fetch_coordinates(self, parsed_address):
        url = f"https://api-adresse.data.gouv.fr/search/?q={parsed_address}"
        response = requests.get(url)
        time.sleep(0.1)
        api_address_informations = {
            "longitude": None,
            "latitude": None,
            "city_code": None,
            "api_adresse_city": None,
        }
        if response.status_code == 200:
            data = response.json()
            try:
                api_address_informations = {
                    "longitude": data["features"][0]["geometry"]["coordinates"][0],
                    "latitude": data["features"][0]["geometry"]["coordinates"][1],
                    "city_code": data["features"][0]["properties"]["citycode"],
                    "api_adresse_city": data["features"][0]["properties"]["city"],
                }
                return api_address_informations
            except:
                return api_address_informations
        return api_address_informations

    def add_coordinates(self):
        dataframe_with_coordinates = self.user_address_dataframe.assign(
            new_data=lambda df: df["parsed_address"].apply(
                lambda parsed_address: self.fetch_coordinates(parsed_address)
            )
        )
        for new_column in ["longitude", "latitude", "city_code", "api_adresse_city"]:
            dataframe_with_coordinates[new_column] = dataframe_with_coordinates[
                "new_data"
            ].apply(lambda data: data[new_column])
        self.user_address_dataframe = dataframe_with_coordinates.drop(
            "new_data", axis=1
        )

    @staticmethod
    def find_commune_informations(user_city_code, communes):
        commune_data = {
            "code_epci": None,
            "epci_name": None,
        }
        if not user_city_code:
            return commune_data
        for i in range(len(communes)):
            commune_code = communes[i]["fields"]["com_code"]
            if user_city_code == commune_code:
                try:
                    commune_data = {
                        "code_epci": communes[i]["fields"]["epci_code"],
                        "epci_name": communes[i]["fields"]["epci_name"],
                    }
                    return commune_data
                except:
                    return commune_data
        return commune_data

    def find_qpv_informations(self, longitude, latitude, user_department_code, qpv):
        qpv_informations = {
            "qpv_communes": None,
            "qpv_name": None,
            "code_qpv": None,
        }
        if longitude is None or latitude is None:
            return qpv_informations
        point = Point(longitude, latitude)
        for i in range(len(qpv)):
            try:
                qpv_department = qpv[i]["fields"]["departement"]
                if user_department_code == qpv_department:
                    commune_coordinate = qpv[i]["fields"]["geo_shape"]["coordinates"][0]
                    polygon = Polygon(commune_coordinate)
                    if point.within(polygon):
                        qpv_informations = {
                            "qpv_communes": qpv[i]["fields"][
                                "noms_des_communes_concernees"
                            ],
                            "qpv_name": qpv[i]["fields"]["nom_qp"],
                            "code_qpv": qpv[i]["fields"]["code_quartier"],
                        }
                        return qpv_informations
            except:
                pass
        return qpv_informations

    def find_zrr_informations(self, user_city_code, zrr):
        if user_city_code:
            for city in zrr:
                commune_code = city["CODGEO"]
                if user_city_code == commune_code:
                    return city["ZRR_SIMP"]
        return None

    def add_commune_epci_qpv(self):
        fs = gcsfs.GCSFileSystem(project=GCP_PROJECT)
        with fs.open(f"{BUCKET_NAME}/functions/georef-france-commune.json") as file:
            communes = json.load(file)

        self.user_address_dataframe[
            ["code_epci", "epci_name"]
        ] = self.user_address_dataframe.apply(
            lambda row: self.find_commune_informations(row["city_code"], communes),
            axis=1,
            result_type="expand",
        )

        with fs.open(
            f"{BUCKET_NAME}/functions/liste_quartiers_prioritairesville.json"
        ) as file:
            qpv = json.load(file)

        self.user_address_dataframe[
            ["qpv_communes", "qpv_name", "code_qpv"]
        ] = self.user_address_dataframe.apply(
            lambda row: self.find_qpv_informations(
                row["longitude"], row["latitude"], row["user_department_code"], qpv
            ),
            axis=1,
            result_type="expand",
        )

        with fs.open(
            f"{BUCKET_NAME}/functions/diffusion-zonages-zrr-2020.csv", "r"
        ) as file:
            reader = csv.DictReader(file)
            zrr = []
            for row in reader:
                zrr.append(row)

        self.user_address_dataframe["zrr"] = self.user_address_dataframe.apply(
            lambda row: self.find_zrr_informations(row["city_code"], zrr), axis=1
        )

    def run(self):
        print("start script")
        print("fetch new user data")
        self.user_address_dataframe = self.fetch_new_user_data()
        print(f"{self.user_address_dataframe.shape[0]} users fetched")
        if self.user_address_dataframe.shape[0] == 0:
            return "No new users !"
        print("add address")
        self.add_parsed_adress()
        print("address added")
        print("add coordinates")
        self.add_coordinates()
        print("coordinates added")
        print("add qpv / commune info")
        self.add_commune_epci_qpv()
        print("qpv / commune info added")
        print("create csv")
        self.user_address_dataframe["date_updated"] = datetime.now().isoformat()
        self.user_address_dataframe[
            [
                "user_id",
                "user_address",
                "user_city",
                "user_postal_code",
                "user_department_code",
                "longitude",
                "latitude",
                "city_code",
                "api_adresse_city",
                "code_epci",
                "epci_name",
                "qpv_communes",
                "qpv_name",
                "code_qpv",
                "zrr",
                "date_updated",
            ]
        ].to_csv(
            f"gcs://{BUCKET_NAME}/{self.user_locations_file_name}",
            index=False,
            sep="|",
        )
        print("csv created")
        return self.user_locations_file_name
