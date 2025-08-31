# ETL script for data ingestion, transformation and loading
# pySpark

import sys
import requests

sys.path.append(".")



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

    # cast all integer to float if any
    data['coord']['lon'] = float(data['coord']['lon'])
    data['coord']['lat'] = float(data['coord']['lat'])

    data['wind']['speed'] = float(data['wind']['speed'])
    data['wind']['gust'] = float(data['wind']['gust'])

    data['main']['feels_like'] = float(data['main']['feels_like'])
    data['main']['temp'] = float(data['main']['temp'])
    data['main']['temp_min'] = float(data['main']['temp_min'])
    data['main']['temp_max'] = float(data['main']['temp_max'])
    

    return data

def extract(region:str):
    weather = []
    if region not in locations:
        print(f"\nRegion not found! The available regions are: \n{', '.join([x for x in locations])}\n")
        return
    for i in locations[region]:
        data = get_weather_data(i)
        weather.append(data)


    return weather



from utils.utils import *

spark = create_spark()


if __name__ == "__main__":
    # get region from arguments 
    print("ETL job started")
    extract("ALL")



    # df = spark.createDataFrame(weather, schema=schema)

    # df.show()
    # df.createOrReplaceTempView("weather")


    # spark.sql("""
    # SELECT * FROM weather    
    # """)

