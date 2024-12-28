from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import requests
import json
from pymongo import MongoClient
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import logging
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from kafka import *
from confluent_kafka import *


def fetch_data():
    API_KEY = "5e7d0c7b0b8ddd4af60bba6e453a22a9"
    
    Cities = [
        {"Name":"Amman", "latitude":31.9497, "longitude":35.9328},
        {"Name":"Zarqa", "latitude":32.0949, "longitude":36.0978},
        {"Name":"Al Mafraq", "latitude":32.3399, "longitude":36.2052},
        {"Name":"Irbid", "latitude":32.5500, "longitude":35.8500},
        {"Name":"Ajloun", "latitude":32.3325, "longitude":35.7517},
        {"Name":"Al Aqabah", "latitude":29.5319, "longitude":35.0056},
        {"Name":"As Salt", "latitude":32.0333, "longitude":35.7333},
        {"Name":"Madaba", "latitude":31.7167, "longitude":35.8000},
        {"Name":"Jarash", "latitude":32.2806, "longitude":35.8972},
        {"Name":"Ma'an", "latitude":30.1962, "longitude":35.7341},
        {"Name":"Al Karak", "latitude":31.1853, "longitude":35.7048},
        {"Name":"At Tafilah", "latitude":30.8400, "longitude":35.6000}
    ]
    
    all_data = {}

    all_data['timestamp'] = datetime.now().isoformat() 
    cities_data = []
    
    for city in Cities:
        longitude = city["longitude"]
        latitude = city["latitude"]
        
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={API_KEY}"
        
        try:
            response = requests.get(url)
            response.raise_for_status() 
            city_data = response.json()
            cities_data.append(city_data)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city['Name']}: {e}")
    
    all_data["data"] = cities_data
    
    return all_data



def store_row_data():
    
    client = MongoClient("mongodb://host.docker.internal:27017")
    db = client["Weather_Project"]
    collection = db["weather_row_data"]
    
    all_data = fetch_data()
    
    collection.insert_one(all_data) 



def kafka_sending():

    logging.getLogger('kafka').setLevel(logging.WARNING)

    kafka_producer = KafkaProducer(
        
        bootstrap_servers=['host.docker.internal:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )



    topic_name = "Weather-pipeline"
    
    all_data = fetch_data()
    
    kafka_producer.send(topic_name ,all_data)



def process_store_data():
    
    spark = (
        SparkSession
        .builder
        .appName("Pyspark_Kafka")
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
        .config("spark.sql.shuffle.partitions", 4)
        .master("local[*]")
        .getOrCreate()
    )

    data_df = (
        spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", "ed-kafka:29092")
        .option("subscribe", "Weather-pipeline")
        .option("startingOffsets", "latest")
        .load()
    )

    df_json = data_df.withColumn("data", expr("cast(value as string)"))

    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("data", ArrayType(
            StructType([
                StructField("Longitude", FloatType(), True),
                StructField("Latitude", FloatType(), True),
                StructField("Weather_Condition", StringType(), True),
                StructField("Weather_Description", StringType(), True),
                StructField("Base", StringType(), True),
                StructField("Temperature", FloatType(), True),
                StructField("Feels_Like", FloatType(), True),
                StructField("Min_Temperature", FloatType(), True),
                StructField("Max_Temperature", FloatType(), True),
                StructField("Pressure", IntegerType(), True),
                StructField("Humidity", IntegerType(), True),
                StructField("Sea_Level_Pressure", IntegerType(), True),
                StructField("Ground_Level_Pressure", IntegerType(), True),
                StructField("Visibility", IntegerType(), True),
                StructField("Wind_Speed", FloatType(), True),
                StructField("Wind_Direction", IntegerType(), True),
                StructField("Clouds", IntegerType(), True),
                StructField("Country", StringType(), True),
                StructField("City", StringType(), True)
            ])
        ), True)
    ])

    parsed_df = df_json.select(from_json(col("data"), schema).alias("data"))
    data_df_parsed = parsed_df.select("data.timestamp", "data.data")

    city_name_corrections = {
    "Adjlun": "Ajloun",
    "Isfīr al Maḩaṭṭah": "Ma'an",   
    "Qīr Moāv": "Al Karak",           
    "Aţ Ţafīlah": "At Tafilah",
    "Salt":"Balqa"
    }

    data_list = [
        {
            "Longitude": data["coord"]["lon"],
            "Latitude": data["coord"]["lat"],
            "Weather_Condition": data["weather"][0]["main"],
            "Weather_Description": data["weather"][0]["description"],
            "Base": data["base"],
            "Temperature": round(data["main"]["temp"] - 273.15, 2),
            "Feels_Like": round(data["main"]["feels_like"] - 273.15, 2),
            "Min_Temperature": round(data["main"]["temp_min"] - 273.15, 2),
            "Max_Temperature": round(data["main"]["temp_max"] - 273.15, 2),
            "Pressure": data["main"]["pressure"],
            "Humidity": data["main"]["humidity"],
            "Sea_Level_Pressure": data["main"]["sea_level"],
            "Ground_Level_Pressure": data["main"]["grnd_level"],
            "Visibility": data["visibility"],
            "Wind_Speed": data["wind"]["speed"],
            "Wind_Direction": data["wind"]["deg"],
            "Clouds": data["clouds"]["all"],
            "Country": data["sys"]["country"],
            "City": city_name_corrections.get(data["name"], data["name"])
        }
        for data in all_data["data"]
    ]

    data_dict = {}
    data_dict["timestamp"] = all_data["timestamp"]
    data_dict["data"] = data_list

    client = MongoClient("mongodb://host.docker.internal:27017")
    db = client["Weather_Project"]
    collection = db["weather_processed_data"]
    collection.insert_one(data_dict)



dag = DAG(
    dag_id="Data_Pipeline",
    start_date=datetime(2024, 12, 10),
    schedule_interval = "@hourly", 
    catchup=False,
)


store_task = PythonOperator(
    task_id="store_raw_data",
    python_callable=store_row_data,
    dag=dag,
)

kafka_task = PythonOperator(
    task_id="kafka_sending",
    python_callable=kafka_sending,
    dag=dag,
)


process_store_task = PythonOperator(
    task_id="process_store_data",
    python_callable=process_store_data,
    dag=dag,
)


store_task >> kafka_task >> process_store_task