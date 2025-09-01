# ETL script for data ingestion, transformation and loading
# pySpark

import sys
import requests

sys.path.append(".")

from utils.utils import *

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
            
        return results # only getting the first one, since the others are irrelavant at times
    
def get_weather_data(city:str):
    results = get_lat_lon(city)
    req = f"https://api.openweathermap.org/data/2.5/weather?lat={results['lat']}&lon={results['lon']}&appid={OWM_API_KEY}"
    data = requests.get(req).json()

    # cast all integer to float if any
    data['coord']['lon'] = float(data['coord']['lon'])
    data['coord']['lat'] = float(data['coord']['lat'])

    data['wind']['speed'] = float(data['wind']['speed'])
    data['wind']['gust'] = float(data['wind']['gust'])

    if('rain' in data):
        data['rain']['1h'] = float(data['rain']['1h'])
        
    if('snow' in data):
        data['snow']['1h'] = float(data['snow']['1h'])

    data['main']['feels_like'] = float(data['main']['feels_like'])
    data['main']['temp'] = float(data['main']['temp'])
    data['main']['temp_min'] = float(data['main']['temp_min'])
    data['main']['temp_max'] = float(data['main']['temp_max'])
    
    data['weather'] = data['weather'][0] # only save the first one
    return data


def flatten_columns(df, exlcude_prefix):
    for prefix in df.columns: 
        newColumns = {}
        if isinstance(df.schema[prefix].dataType, StructType): # check if column is nested column of the type struct
            for fields in df.schema[prefix].dataType:
                if(prefix in exlcude_prefix):
                    newColName = f"{fields.name}"
                else:
                    newColName = f"{prefix}_{fields.name}"
                    
                column = f"{prefix}.{fields.name}"
                newColumns[newColName] = column

            # add new columns for each column 
            df = df.withColumns(newColumns)\
            .drop(prefix) # drop original column


    return df


def extract(region:str):
    weather = []
    if region not in locations:
        print(f"\nRegion not found! The available regions are: \n{', '.join([x for x in locations])}\n")
        return
    for i in locations[region]:
        data = get_weather_data(i)
        weather.append(data)


    return weather


def transform(weather):
    df = spark.createDataFrame(weather, schema=schema)
    df = flatten_columns(df, exlcude_prefix=["main", "sys", "coord"])

    df.show(5)
    



if __name__ == "__main__":
    # get region from arguments in next change
    print("ETL job started")
    spark = create_spark()


    weather = extract("ALL")
    transform(weather)





