# ETL script for data ingestion, transformation and loading
# pySpark

import requests 
from utils.utils import *

spark = create_spark()


def get_lat_lon(city: str) -> dict:
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
    
def get_weather_data(city:str) -> dict:
    results = get_lat_lon(city)
    req = f"https://api.openweathermap.org/data/2.5/weather?lat={results['lat']}&lon={results['lon']}&appid={OWM_API_KEY}&units=metric"
    data = requests.get(req).json()

    # cast all integer to float if any
    data['coord']['lon'] = float(data['coord']['lon'])
    data['coord']['lat'] = float(data['coord']['lat'])

    if 'speed' in data['wind']:
        data['wind']['speed'] = float(data['wind']['speed'])

    if 'gust' in data['wind']:
        data['wind']['gust'] = float(data['wind']['gust'])

    if('rain' in data):
        data['rain']['1h'] = float(data['rain']['1h'])
        
    if('snow' in data):
        data['snow']['1h'] = float(data['snow']['1h'])

    if 'feels_like' in data['main']:
        data['main']['feels_like'] = float(data['main']['feels_like'])

    if 'temp' in data['main']:
        data['main']['temp'] = float(data['main']['temp'])

    if 'temp_min' in data['main']:
        data['main']['temp_min'] = float(data['main']['temp_min'])

    if 'temp_max' in data['main']:
        data['main']['temp_max'] = float(data['main']['temp_max'])
    
    data['weather'] = data['weather'][0] # only save the first one
    return data


def flatten_columns(df:DataFrame, exlcude_prefix:list) -> DataFrame:
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

def convert_datetime(df:DataFrame, datetimes:list, timezone:str) -> DataFrame:
    convert_datetimes_UTC = {}
    rename_datetimes_UTC = {}

    for columns in datetimes:
        if columns in df.columns:
            convert_datetimes_UTC[columns] = from_unixtime(columns)
            rename_datetimes_UTC[columns] = f"{columns} (UTC)"
        else:
            print(f"{columns} is not found in Dataframe. Ignored")
    
    
    df = df.withColumns(convert_datetimes_UTC)\
        .withColumnsRenamed(rename_datetimes_UTC)

    if timezone:
        df = df.withColumn(timezone, col(timezone)/3600)
        return df

    return df

def reorder_columns(df:DataFrame, orderByID:bool) -> DataFrame:
    # drop internal params (cod, base)
    df = df.select("id","name","country","lon","lat",\
                    "timezone","dt (UTC)","sunrise (UTC)","sunset (UTC)",\
                    "weather_id","weather_main","weather_description","weather_icon",\
                    "temp","feels_like","temp_min","temp_max",\
                    "pressure","humidity","sea_level","grnd_level",\
                    "wind_speed","wind_deg","wind_gust","rain_1h","snow_1h","clouds_all","visibility")\
    
    if orderByID:
        df = df.orderBy("id")

    return df

def extract(region:str) -> list:
    weather = []
    if region not in locations:
        print(f"\nRegion not found! The available regions are: \n{', '.join([x for x in locations])}\n")
        return
    for i in locations[region]:
        data = get_weather_data(i)
        weather.append(data)


    return weather


def transform(weather:list) -> DataFrame:
    df = spark.createDataFrame(weather, schema=schema)
    df = flatten_columns(df, exlcude_prefix=["main", "sys", "coord"])
    df = convert_datetime(df, timezone="timezone", datetimes=["dt", "sunset", "sunrise"])
    df = reorder_columns(df, orderByID=False)
    
    return df



if __name__ == "__main__":
    # get region from arguments in next change
    print("ETL job started")

    weather = extract("ALL")
    df = transform(weather)
    # df.show(5)
    df_select = df.select("id","name","country",\
                      "timezone","dt (UTC)",\
                      "weather_description",\
                      "temp",\
                      "pressure","humidity","sea_level","grnd_level",\
                      "wind_speed","rain_1h","snow_1h","visibility").show()





