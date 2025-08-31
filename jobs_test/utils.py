import requests
import os
from dotenv import load_dotenv
from pyspark.sql.types import *


load_dotenv()


OWM_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
schema = StructType([
    StructField("coord", StructType([StructField("lon", DoubleType(), True), StructField("lat", DoubleType(), True)])),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True)
    ]))),
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


def get_lat_lon(city: str):
    req = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=5&appid={OWM_API_KEY}"
    data = requests.get(req).json()
    results = {}
    for i in data: # i is dict in the list (can be more)
        for j in i: # j is the keys of the dictionary 
            if(j=="lat" or j=="lon"):
                results[j] = i[j]
            else:
                continue
            
        return results
            


def get_weather_data(city:str):
    results = get_lat_lon(city)
    req = f"https://api.openweathermap.org/data/2.5/weather?lat={results['lat']}&lon={results['lon']}&appid={OWM_API_KEY}"
    data = requests.get(req).json()
    # print("==========================================")
    # for i in data:
        # print(f"{i}: {data[i]}")
    # print(data)

    # cast all integer to float if any
    data['coord']['lon'] = float(data['coord']['lon'])
    data['coord']['lat'] = float(data['coord']['lat'])

    data['wind']['speed'] = float(data['wind']['speed'])
    data['wind']['gust'] = float(data['wind']['gust'])

    data['main']['feels_like'] = float(data['main']['feels_like'])
    data['main']['temp'] = float(data['main']['temp'])
    data['main']['temp_min'] = float(data['main']['temp_min'])
    data['main']['temp_max'] = float(data['main']['temp_max'])
    # print("==========================================")
    return data
