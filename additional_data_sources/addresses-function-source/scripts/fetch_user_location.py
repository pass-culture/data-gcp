import csv
import json
import os
import time
from urllib.parse import quote
import requests
import gcsfs
from shapely.geometry import Point, Polygon
from scripts.bigquery_client import BigQueryClient

bigquery_client = BigQueryClient()

GCP_PROJECT = os.environ["PROJECT_NAME"]
BIGQUERY_RAW_DATASET = os.environ["BIGQUERY_RAW_DATASET"]
BIGQUERY_CLEAN_DATASET = os.environ["BIGQUERY_CLEAN_DATASET"]


class AdressesDownloader:
    def __init__(self, project_name, user_locations_file_name):
        self.project_name = project_name
        self.user_locations_file_name = user_locations_file_name
        self.user_address_dataframe = None

    def fetch_new_user_data(self):
        bigquery_query = f"""
        SELECT user_id, user_address, user_postal_code, user_city, user_department_code
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_user`
        WHERE user_address is not NULL AND user_address <> ""
        AND user_id not in (SELECT user_id FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.user_locations`);
        """

        user_address_dataframe = bigquery_client.query(bigquery_query)
        return user_address_dataframe

    def add_parsed_adress(self):
        self.user_address_dataframe["parsed_address"] = self.user_address_dataframe[
            ["user_address", "user_postal_code", "user_city"]
        ].apply(
            lambda row: quote(" ".join(row)),
            axis=1,
        )

    def fetch_coordinates(self, parsed_address):
        url = f"https://api-adresse.data.gouv.fr/search/?q={parsed_address}"
        response = requests.get(url)
        time.sleep(0.1)
        api_address_informations = []
        if response.status_code == 200:
            data = response.json()
            try:
                longitude = data["features"][0]["geometry"]["coordinates"][0]
                latitude = data["features"][0]["geometry"]["coordinates"][1]
                city_code = data["features"][0]["properties"]["citycode"]
                city = data["features"][0]["properties"]["city"]
                api_address_informations.append(longitude)
                api_address_informations.append(latitude)
                api_address_informations.append(city_code)
                api_address_informations.append(city)
                return api_address_informations
            except:
                return [None, None, None, None]
        return [None, None, None, None]

    def add_coordinates(self):
        self.user_address_dataframe[
            ["longitude", "latitude", "city_code", "api_adresse_city"]
        ] = self.user_address_dataframe.apply(
            lambda row: self.fetch_coordinates(row["parsed_address"]),
            axis=1,
            result_type="expand",
        )

    def find_commune_informations(self, user_city_code, communes):
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

    def find_qpv_informations(self, user_coordinates, user_department_code, qpv):
        qpv_informations = {
            "qpv_communes": None,
            "qpv_name": None,
            "code_qpv": None,
        }
        if (
            user_coordinates["longitude"] is None
            or user_coordinates["latitude"] is None
        ):
            return qpv_informations
        point = Point(user_coordinates["longitude"], user_coordinates["latitude"])
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
        fs = gcsfs.GCSFileSystem(project=self.project_name)
        with fs.open("data-bucket-dev/functions/georef-france-commune.json") as file:
            communes = json.load(file)
        with fs.open(
            "data-bucket-dev/functions/liste_quartiers_prioritairesville.json"
        ) as file:
            qpv = json.load(file)
        with fs.open(
            "data-bucket-dev/functions/diffusion-zonages-zrr-2020.csv", "r"
        ) as file:
            reader = csv.DictReader(file)
            zrr = []
            for row in reader:
                zrr.append(row)

        self.user_address_dataframe["code_epci"] = None
        self.user_address_dataframe["epci_name"] = None
        self.user_address_dataframe["qpv_communes"] = None
        self.user_address_dataframe["qpv_name"] = None
        self.user_address_dataframe["code_qpv"] = None
        self.user_address_dataframe["zrr"] = None
        for i in range(self.user_address_dataframe.shape[0]):
            user_coordinates = {
                "longitude": self.user_address_dataframe["longitude"].loc[i],
                "latitude": self.user_address_dataframe["latitude"].loc[i],
            }
            user_department_code = self.user_address_dataframe[
                "user_department_code"
            ].loc[i]
            user_city_code = self.user_address_dataframe["city_code"].loc[i]

            commune_informations = self.find_commune_informations(
                user_city_code, communes
            )
            self.user_address_dataframe["code_epci"].loc[i] = commune_informations[
                "code_epci"
            ]
            self.user_address_dataframe["epci_name"].loc[i] = commune_informations[
                "epci_name"
            ]

            qpv_informations = self.find_qpv_informations(
                user_coordinates, user_department_code, qpv
            )
            self.user_address_dataframe["qpv_communes"].loc[i] = qpv_informations[
                "qpv_communes"
            ]
            self.user_address_dataframe["qpv_name"].loc[i] = qpv_informations[
                "qpv_name"
            ]
            self.user_address_dataframe["code_qpv"].loc[i] = qpv_informations[
                "code_qpv"
            ]

            self.user_address_dataframe["zrr"].loc[i] = self.find_zrr_informations(
                user_city_code, zrr
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
            ]
        ].to_csv(
            "gcs://" + self.user_locations_file_name,
            index=False,
            sep="|",
        )
        print("csv created")
        return "Addresses added"
