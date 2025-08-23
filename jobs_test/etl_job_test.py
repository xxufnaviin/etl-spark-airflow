# ETL script for data ingestion, transformation and loading
# pySpark

import requests
import os

# os.environ["SPARK_HOME"] = "C:\\spark-4.0.0-bin-hadoop3"
# os.environ["PYSPARK_PYTHON"] = ".venv/Scripts/python.exe"



from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ETL Pipeline").config("spark.driver.memory", "2g").getOrCreate()

print(spark.sparkContext.uiWebUrl)

df = spark.read.csv("jobs/test.csv",header=True, inferSchema=True)
df.createOrReplaceTempView("titanic") #create temp view
spark.sql("""
SELECT Name, Ticket FROM titanic
WHERE AGE >= 50
"""
).show()
spark.catalog.dropTempView("titanic") #remove temp view


# input("Press Enter to stop Spark...")  # keeps process alive
spark.stop() 



# if __name__ == "__main__":
#     print("ETL job started")