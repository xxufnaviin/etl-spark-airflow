# ETL script for data ingestion, transformation and loading
# pySpark

import requests
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from utils.utils import get_weather_data


locations = {
    "SEA": ["Singapore", "Bangkok", "Jakarta", "Kuala Lumpur", "Manila", "Hanoi", "Ho Chi Minh City"],
    "NA" : ["New York", "Los Angeles", "Chicago", "Toronto", "Mexico City", "Houston", "Miami"],
    "SA" : ["São Paulo", "Buenos Aires", "Rio de Janeiro", "Lima", "Bogotá", "Santiago", "Caracas"],
    "EU" : ["London", "Paris", "Berlin", "Madrid", "Rome", "Amsterdam", "Vienna"],
    "AS" : ["Tokyo", "Beijing", "Seoul", "Mumbai", "Shanghai", "Bangkok", "Delhi"],
    "AF" : ["Cairo", "Lagos", "Johannesburg", "Nairobi", "Casablanca", "Accra", "Addis Ababa"],
    "OC" : ["Sydney", "Melbourne", "Auckland", "Brisbane", "Perth", "Fiji", "Port Moresby"], 
    "ALL": []
}

locations["ALL"] = (locations["SEA"] + locations["NA"] + locations["SA"] + locations["EU"] + locations["AS"] + locations["AF"] + locations["OC"])


if __name__ == "__main__":
    print("ETL job started")
    for i in locations["ALL"]:
        get_weather_data(i)