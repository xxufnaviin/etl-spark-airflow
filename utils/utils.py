
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import *


load_dotenv()


OWM_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
schema = StructType([
    StructField("coord", StructType([StructField("lon", DoubleType(), True), StructField("lat", DoubleType(), True)])),
    StructField("weather", StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True)
    ])),
    StructField("base", StringType(), True),
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("sea_level", IntegerType(), True),
        StructField("grnd_level", IntegerType(), True)
    ])),
    StructField("visibility", IntegerType(), True),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True),
        StructField("deg", IntegerType(), True),
        StructField("gust", DoubleType(), True)
    ])),
    StructField("rain", StructType([StructField("1h", DoubleType(), True)]), True),
    StructField("snow", StructType([StructField("1h", DoubleType(), True)]), True),
    StructField("clouds", StructType([
        StructField("all", IntegerType(), True)
    ])),
    StructField("dt", LongType(), True),
    StructField("sys", StructType([
        StructField("country", StringType(), True),
        StructField("sunrise", LongType(), True),
        StructField("sunset", LongType(), True)
    ])),
    StructField("timezone", IntegerType(), True),
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("cod", IntegerType(), True)
])

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


def create_spark():
    spark = SparkSession.builder.appName("ETL Pipeline").config("spark.driver.memory", "2g").getOrCreate()
    return spark



