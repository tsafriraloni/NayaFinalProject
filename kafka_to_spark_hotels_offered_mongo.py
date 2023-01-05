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
hotels_topic = c.topic6
#news_information_topic = 'kafka-news-information-topic'

####  Creating hotels Spark Session ####
hotels = SparkSession\
        .builder\
        .appName("hotels")\
        .getOrCreate()

#### ReadStream From hotels Kafka Topic ####
df_kafka_hotels = hotels\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option("subscribe", hotels_topic)\
    .load()



#### Creating a Schema for hotels Spark Structured Streaming ####
hotels_schema = StructType() \
    .add("location_arrival", StringType())\
    .add("date_departure", StringType())\
    .add("location_departure", StringType())\
    .add("date_departure_return", StringType())\
    .add("price", StringType())\
    .add("currencyCode", StringType())\
    .add("hotel", StringType())\
    .add("totalTripDurationInHours", StringType())\
    .add("user_email", StringType())


### Change Json To Dataframe With hotels Schema ####
df_kafka_hotels = df_kafka_hotels.select(col("value").cast("string"))\
    .select(from_json(col("value"), hotels_schema).alias("value"))\
    .select("value.*")


#### Adding Calculated Columns To Spark Data Frame ####
df_kafka_hotels = df_kafka_hotels.withColumn("current_ts", current_timestamp().cast('string'))
df_kafka_hotels= df_kafka_hotels.withColumn("is_active", lit(1))



print(df_kafka_hotels)

#df_kafka_hotels = target_df

target_df = df_kafka_hotels

#target_df.show



############## truncate hotel table mongodb ##############
#mogodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
#mydb = mogodb_client["travel_app"]
#mycol = mydb["Hotels"]
#x = mycol.delete_many({})
############################################################

#### Defining A Function To Send Dataframe To MongoDB ####
def write_df_mongo_enriched_data(target_df):

    mogodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = mogodb_client["travel_app"]
    mycol = mydb["Hotels"]
    #x = mycol.delete_many({})
    post = {
        "location_arrival": target_df.location_arrival,
        "date_departure": target_df.date_departure,
        "location_departure": target_df.location_departure,
        "date_departure_return": target_df.date_departure_return,
        "price": target_df.price,
        "currencyCode": target_df.currencyCode,
        "hotel": target_df.hotel,
        "price": target_df.price,
        "currencyCode": target_df.currencyCode,
        "hotel": target_df.hotel,
        "totalTripDurationInHours": target_df.totalTripDurationInHours,
        "user_email": target_df.user_email,
        "current_ts": target_df.current_ts,
        "is_active": target_df.is_active


    }

    mycol.insert_one(post)
    #print('item inserted')
#### Spark Action ###
df_kafka_hotels \
    .writeStream \
    .foreach(write_df_mongo_enriched_data)\
    .outputMode("append") \
    .start() \
    .awaitTermination()
df_kafka_hotels.show(truncate=False)

