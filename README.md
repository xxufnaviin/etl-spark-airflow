# 🌤️ Weather ETL - using Apache Airflow and Apache Spark

This project implements an **ETL (Extract, Transform, Load) pipeline** to collect, process, and store weather data from the **OpenWeatherMap API**. The pipeline leverages **PySpark** for data transformation, **Apache Airflow** for orchestration, and **Google Cloud Storage (GCS)** for cloud storage.


## Project Overview

The pipeline performs the following steps:

1. **Extract**: Retrieves real-time weather data from OpenWeatherMap API for multiple cities (can choose between regions) 
2. **Transform**: Processes and cleans the data using PySpark, including flattening nested JSON and converting timestamps to local time.  
3. **Load**: Saves the processed data as CSV and uploads them to Google Cloud Storage as an object in a bucket.  
4. **Orchestration**: Automates scheduling and monitoring of the ETL workflow using Airflow.


### Architecture
<img width="929" height="578" alt="image" src="https://github.com/user-attachments/assets/2be4ce56-2ba4-43d1-8a0e-92b30748ec3e" />

### Technologies Used
- **Python** 
- **PySpark** 
- **Apache Airflow** 
- **Google Cloud Storage (GCS)** 
- **API Calls** 

### Next Steps‼️
- Monitor performance and **handle errors** that originates from API - including data format changes
- Perform **data analysis** on the data and make prediction 
- **Containerize the pipeline** for easier deployment and scalability


### Example Airflow Orchestration + Saving to Google Cloud Storage


