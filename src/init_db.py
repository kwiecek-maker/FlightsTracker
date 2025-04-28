"""A module to retrieve data from the API, process them and save them to a No-SQL database"""

import copy
from pymongo import MongoClient
import requests
import json

with open("config.json") as f:
    cfg = json.load(f)


class ApiToDatabaseApp:
    def __init__(self, api_url, db_url, **api_params):
        self.api_url = api_url
        self.api_params = api_params
        self.db_url = db_url
        self.cli = self._connect_db()
        self.db_name = ""
        self.collection_name = ""

    def _connect_db(self):
        cli = MongoClient(self.db_url)
        return cli

    def get_api_data(self, *args):
        """get data from API"""
        print(self.api_params)
        response = requests.get(self.api_url, **self.api_params).json()
        return response

    def process_api_data(self, *data):
        return None

    def add_data_to_db(self, data: list[dict]):
        db = self.cli[self.db_name]
        collection = db[self.collection_name]
        print(f"db={self.db_name}, collection={self.collection_name}")
        _ = collection.insert_many(data)


class ApiAirportsToDatabase(ApiToDatabaseApp):
    """ Get API from https://sky-scrapper.p.rapidapi.com/api/v1/flights/searchAirport"""
    def __init__(self, api_url, db_url, **api_params):
        super().__init__(api_url, db_url, **api_params)
        self.db_name = "flights"
        self.collection_name = "airports"
        self.airports = []

    def get_api_data(self, countries: list = None) -> list[dict]:
        if countries is None:
            countries = cfg["app"]["countries"]
        for country in countries:
            self.api_params["params"] = {"query": country}
            data = super().get_api_data()
            self.process_api_data(data, country)
        return self.airports

    def process_api_data(self, data: dict, country: str):
        data = data["data"]
        for el in data:
            new_airport = copy.deepcopy(cfg["database"]["collections"]["airports_schema"])
            new_airport["skyId"] = el["skyId"]
            new_airport["entityId"] = el["entityId"]
            new_airport["entityType"] = el["navigation"]["entityType"]
            new_airport["localizedName"] = el["navigation"]["localizedName"]
            new_airport["countryName"] = country
            self.airports.append(new_airport)

    def add_data_to_db(self, data: list[dict]):
        super().add_data_to_db(data)


if __name__ == '__main__':
    user = cfg["database"]["user"]
    password = cfg["database"]["password"]
    host = cfg["database"]["host"]
    url_db = f"mongodb+srv://{user}:{password}@{host}.mongodb.net/"
    url_api = cfg["api_connection"]["airports_url"]
    params_api = {"headers": cfg["api_connection"]["headers"]}

    airports_obj = ApiAirportsToDatabase(api_url=url_api, db_url=url_db, **params_api)
    airports_data = airports_obj.get_api_data()
    airports_obj.add_data_to_db(airports_data)
