# ETL script for data ingestion, transformation and loading
# pySpark

import requests
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession


load_dotenv()

# os.environ["SPARK_HOME"] = "C:\\spark-4.0.0-bin-hadoop3"
# os.environ["PYSPARK_PYTHON"] = ".venv/Scripts/python.exe"

OWM_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
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
locations["ALL"] = (locations["SEA"] +locations["NA"] +locations["SA"] +locations["EU"] +locations["AS"] +locations["AF"] +locations["OC"])


def get_lat_lon(city: str):
    req = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=5&appid={OWM_API_KEY}"
    data = requests.get(req).json()
    results = {}
    # print(data)
    for i in data: # i is dict in the list (can be more)
        for j in i: # j is the keys of the dictionary 
            if(j=="lat" or j=="lon"):
                # print(f"{j}: {i[j]}")
                results[j] = i[j]
            else:
                continue
            
        return results
            


def get_weather_data(city:str):
    results = get_lat_lon(city)
    req = f"https://api.openweathermap.org/data/2.5/weather?lat={results["lat"]}&lon={results["lon"]}&appid={OWM_API_KEY}"
    data = requests.get(req).json()

    print("==========================================")
    for i in data:
        print(f"{i}: {data[i]}")
    print("==========================================")

# spark = SparkSession.builder.appName("ETL Pipeline").config("spark.driver.memory", "2g").getOrCreate()

# print(spark.sparkContext.uiWebUrl)

# df = spark.read.csv("jobs/test.csv",header=True, inferSchema=True)
# df.createOrReplaceTempView("titanic") #create temp view
# spark.sql("""
# SELECT Name, Ticket FROM titanic
# WHERE AGE >= 50
# """
# ).show()
# spark.catalog.dropTempView("titanic") #remove temp view


# spark.stop() 


if __name__ == "__main__":
    print("ETL job started")
    for i in locations["ALL"]:
        get_weather_data(i)