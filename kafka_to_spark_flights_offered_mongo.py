import pymongo
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType
from pyspark.sql import SparkSession
import configuration as c

import datetime
from datetime import date
import pandas as pd
import json
import boto3

### Creating a Conection Between Spark And Kafka ####
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
bootstrapServers = "cnt7-naya-cdh63:9092"
flights_topic = c.topic5


####  Creating Flights Spark Session ####
flights = SparkSession\
        .builder\
        .appName("flights")\
        .getOrCreate()

####  Creating News Spark Session ####
#news = SparkSession\
#        .builder\
#        .appName("news")\
#        .getOrCreate()

#### ReadStream From flights Kafka Topic ####
df_kafka_flights = flights\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option("subscribe", flights_topic)\
    .load()

#### ReadStream From News Kafka Topic ####
#df_kafka_news = news\
#    .readStream\
#    .format("kafka")\
#    .option("kafka.bootstrap.servers", bootstrapServers)\
#    .option("subscribe", news_information_topic)\
#    .load()



#### Creating a Schema for Flights Spark Structured Streaming ####
flights_schema = StructType() \
    .add("location_arrival", StringType())\
    .add("date_departure", StringType())\
    .add("location_departure", StringType())\
    .add("date_departure_return", StringType())\
    .add("price", StringType())\
    .add("currencyCode", StringType())\
    .add("carrier", StringType())\
    .add("totalTripDurationInHours", StringType())\
    .add("user_email", StringType())

#### Creating a Schema for News Spark Structured Streaming ####
#news_schema = StructType() \
#    .add("chat_id", StringType())\
#    .add("news_summary", StringType())\
#    .add("news_link", StringType())



### Change Json To Dataframe With Flights Schema ####
df_kafka_flights = df_kafka_flights.select(col("value").cast("string"))\
    .select(from_json(col("value"), flights_schema).alias("value"))\
    .select("value.*")

### Change Json To Dataframe With News Schema ####
#df_kafka_news = df_kafka_news.select(col("value").cast("string"))\
#    .select(from_json(col("value"), news_schema).alias("value"))\
#    .select("value.*")

#### Adding Calculated Columns To Spark Data Frame ####
df_kafka_flights = df_kafka_flights.withColumn("current_ts", current_timestamp().cast('string'))
df_kafka_flights= df_kafka_flights.withColumn("is_active", lit(1))

#print(df_kafka)

#### Join All The DataFrames ######

#target_df = df_kafka_flights.join(df_kafka_news, "chat_id")



print(df_kafka_flights)

#df_kafka_flights = target_df

target_df = df_kafka_flights

#target_df.show



############## truncate flight table mongodb ##############
#mogodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
#mydb = mogodb_client["travel_app"]
#mycol = mydb["Flights"]
#x = mycol.delete_many({})
############################################################

#### Defining A Function To Send Dataframe To MongoDB ####
def write_df_mongo_enriched_data(target_df):

    mogodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = mogodb_client["travel_app"]
    mycol = mydb["Flights"]
    #x = mycol.delete_many({})
    post = {
        "location_arrival": target_df.location_arrival,
        "date_departure": target_df.date_departure,
        "location_departure": target_df.location_departure,
        "date_departure_return": target_df.date_departure_return,
        "price": target_df.price,
        "currencyCode": target_df.currencyCode,
        "carrier": target_df.carrier,
        "price": target_df.price,
        "currencyCode": target_df.currencyCode,
        "carrier": target_df.carrier,
        "totalTripDurationInHours": target_df.totalTripDurationInHours,
        "user_email": target_df.user_email,
        "current_ts": target_df.current_ts,
        "is_active": target_df.is_active
        #,"news_summary": target_df.news_summary,
        #,"news_link": target_df.news_link

    }

    mycol.insert_one(post)
    #print('item inserted')
#### Spark Action ###
df_kafka_flights \
    .writeStream \
    .foreach(write_df_mongo_enriched_data)\
    .outputMode("append") \
    .start() \
    .awaitTermination()
df_kafka_flights.show(truncate=False)

