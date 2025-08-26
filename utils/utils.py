import requests
import os
from dotenv import load_dotenv



load_dotenv()

# os.environ["SPARK_HOME"] = "C:\\spark-4.0.0-bin-hadoop3"
# os.environ["PYSPARK_PYTHON"] = ".venv/Scripts/python.exe"

OWM_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")


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
