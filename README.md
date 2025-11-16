# 🌤️ Weather ETL - using Apache Airflow and Apache Spark

This project implements an **ETL (Extract, Transform, Load) pipeline** to collect, process, and store weather data from the **OpenWeatherMap API**. The pipeline leverages **PySpark** for data transformation, **Apache Airflow** for orchestration, **Docker** for custom image and containerization, and **Google Cloud Storage (GCS)** for cloud storage.


## Project Overview

The pipeline performs the following steps:

1. **Extract**: Retrieves real-time weather data from OpenWeatherMap API for multiple cities (can choose between regions) 
2. **Transform**: Processes and cleans the data using PySpark, including flattening nested JSON and converting timestamps to local time.  
3. **Load**: Saves the processed data as CSV and uploads them to Google Cloud Storage as an object in a bucket.  
4. **Orchestration**: Automates scheduling and monitoring of the ETL workflow using Airflow.
5. **Custom Docker Image**: Allow scripts to run on Airflow base image on top of Spark + Java + Python dependencies.
6. **Containerization**: Allow local development and testing with docker compose, easier for deployment.


### Architecture
<img width="929" height="578" alt="image" src="https://github.com/user-attachments/assets/2be4ce56-2ba4-43d1-8a0e-92b30748ec3e" />

### Technologies Used
- **Python** 
- **PySpark** 
- **Apache Airflow** 
- **Google Cloud Storage (GCS)** 
- **Docker**
- **Github Container Registry**

## Instructions for local development
### 1. Pull Docker Image
Install docker 
```bash
docker pull ghcr.io/xxufnaviin/etl-spark-airflow:263.310.4
```
### 2. Add Google Cloud Credentials
Place your Google Service Account JSON in the project directory (utils/your-google-creds.json) and ensure the path matches the one in .env.<br>
NOTE: must place it under utils/

### 3. Set Environment Variables
Get API Key for free: https://openweathermap.org/
```bash
OPENWEATHERMAP_API_KEY="REPLACE THIS WITH YOUR OWN API KEY FROM OPENWEATHERMAP"
GOOGLE_APPLICATION_CREDENTIALS="../utils/your-google-creds.json"
```
NOTE: DO NOT CHANGE THE FILE PATH, ONLY REPLACE JSON FILE NAME

### 4. Start Airflow Services
```bash
docker-compose up
```
This starts airflow db-init to initialize local database, airflow webserver and airflow scheduler.

### 5. Access Webserver (Airflow UI)
```bash
http://localhost:8080
```
username:admin<br>
password:admin123

### 6. Exit services
```bash
docker-compose down
```

IMPORTANT: Replace BUCKET_NAME with your own bucket in airflow/dags/dag.py-L19 <br>
Contact me for any issues: https://www.linkedin.com/in/xxufnaviin/

### 
### Next Steps‼️
- Monitor performance and **handle errors** that originates from API - including data format changes
- Perform **data analysis** on the data and make prediction
- Build **End to End frontend + backend application** for weather forecast and analysis result. 




