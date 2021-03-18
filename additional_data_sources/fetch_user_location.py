from urllib.parse import quote
import json
import time

import requests
from shapely.geometry import Point, Polygon

from orchestration.dags.dependencies.bigquery_client import BigQueryClient

bigquery_client = BigQueryClient()


def fetch_user():
    bigquery_query = f"""SELECT user_id, user_address, user_postal_code, user_city FROM `passculture-data-ehp.clean_stg.applicative_database_user`
                            WHERE user_address is not NULL AND user_address <> ""
                            ORDER BY user_id
                      """

    bigquery_result = bigquery_client.query(bigquery_query)
    return bigquery_result


def add_parsed_adress(user_adress_dataframe):
    user_adress_dataframe["parsed_address"] = user_adress_dataframe[
        ["user_address", "user_postal_code", "user_city"]
    ].apply(
        lambda row: quote(" ".join(row)),
        axis=1,
    )
    return user_adress_dataframe


def fetch_coordinates(parsed_address):
    url = "https://api-adresse.data.gouv.fr/search/?q=" + parsed_address
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        try:
            api_adress_informations = {
                "longitude": data["features"][0]["geometry"]["coordinates"][0],
                "latitude": data["features"][0]["geometry"]["coordinates"][1],
            }
        except:
            api_adress_informations = {
                "longitude": None,
                "latitude": None,
            }
    else:
        api_adress_informations = {
            "longitude": None,
            "latitude": None,
        }
    return api_adress_informations


def add_coordinates(user_adress_dataframe):
    user_adress_dataframe["longitude"] = None
    user_adress_dataframe["latitude"] = None
    for i in range(len(user_adress_dataframe.index)):
        coordinates = fetch_coordinates(user_adress_dataframe["parsed_address"].loc[i])
        user_adress_dataframe["longitude"].loc[i] = coordinates["longitude"]
        user_adress_dataframe["latitude"].loc[i] = coordinates["latitude"]
        time.sleep(0.1)


def find_commune_informations(user_coordinates, communes):
    commune_data = {
        "nom_commune": None,
        "code_epci": None,
        "epci_name": None,
    }
    if user_coordinates["longitude"] is None or user_coordinates["latitude"] is None:
        return commune_data
    point = Point(user_coordinates["longitude"], user_coordinates["latitude"])
    for i in range(len(communes)):
        commune_coordinate = communes[i]["fields"]["geo_shape"]["coordinates"][0]
        try:
            polygon = Polygon(commune_coordinate)
            if point.within(polygon):
                commune_data = {
                    "nom_commune": communes[i]["fields"]["com_name_upper"],
                    "code_epci": communes[i]["fields"]["epci_code"],
                    "epci_name": communes[i]["fields"]["epci_name"],
                }
                return commune_data
        except:
            pass
    return commune_data


def find_qpv_informations(user_coordinates, qpv):
    qpv_informations = {
        "qpv_communes": None,
        "qpv_name": None,
        "code_qpv": None,
    }
    if user_coordinates["longitude"] is None or user_coordinates["latitude"] is None:
        return qpv_informations
    point = Point(user_coordinates["longitude"], user_coordinates["latitude"])
    for i in range(len(qpv)):
        try:
            commune_coordinate = qpv[i]["fields"]["geo_shape"]["coordinates"][0]
            polygon = Polygon(commune_coordinate)
            if point.within(polygon):
                qpv_informations = {
                    "qpv_communes": qpv[i]["fields"]["noms_des_communes_concernees"],
                    "qpv_name": qpv[i]["fields"]["nom_qp"],
                    "code_qpv": qpv[i]["fields"]["code_quartier"],
                }
                return qpv_informations
        except:
            pass
    return qpv_informations


def add_commune_epci_qpv(user_adress_dataframe):
    file = open(r"./georef-france-commune.json")
    communes = json.load(file)
    file = open(r"./liste_quartiers_prioritairesville.json")
    qpv = json.load(file)
    user_adress_dataframe["nom_commune"] = None
    user_adress_dataframe["code_epci"] = None
    user_adress_dataframe["epci_name"] = None
    user_adress_dataframe["qpv_communes"] = None
    user_adress_dataframe["qpv_name"] = None
    user_adress_dataframe["code_qpv"] = None

    for i in range(len(user_adress_dataframe.index)):
        user_coordinates = {
            "longitude": user_adress_dataframe["longitude"].loc[i],
            "latitude": user_adress_dataframe["latitude"].loc[i],
        }
        commune_informations = find_commune_informations(user_coordinates, communes)
        user_adress_dataframe["nom_commune"].loc[i] = commune_informations[
            "nom_commune"
        ]
        user_adress_dataframe["code_epci"].loc[i] = commune_informations["code_epci"]
        user_adress_dataframe["epci_name"].loc[i] = commune_informations["epci_name"]

        qpv_informations = find_qpv_informations(user_coordinates, qpv)
        user_adress_dataframe["qpv_communes"].loc[i] = qpv_informations["qpv_communes"]
        user_adress_dataframe["qpv_name"].loc[i] = qpv_informations["qpv_name"]
        user_adress_dataframe["code_qpv"].loc[i] = qpv_informations["code_qpv"]


def main():
    print("start script")
    print("fetch user")
    user_adress_dataframe = fetch_user()
    print("user fetched")
    print("add address")
    add_parsed_adress(user_adress_dataframe)
    print("address added")
    print("add coordinates")
    add_coordinates(user_adress_dataframe)
    print("coordinates added")
    print("add qpv / commune info")
    add_commune_epci_qpv(user_adress_dataframe)
    print("qpv / commune info added")
    print("create csv")
    user_adress_dataframe[
        [
            "user_id",
            "user_address",
            "user_city",
            "longitude",
            "latitude",
            "code_epci",
            "epci_name",
            "qpv_communes",
            "qpv_name",
            "code_qpv",
        ]
    ].to_csv("./user_locations.csv", index=False, sep=";")
    print("csv created")


if __name__ == "__main__":
    main()
