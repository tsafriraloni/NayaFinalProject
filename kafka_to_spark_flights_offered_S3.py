import pymongo
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType
from pyspark.sql import SparkSession
import configuration as c

import datetime
import time
from datetime import date
import pandas as pd
import json
import boto3
import jsonlines

### Creating a Conection Between Spark And Kafka ####
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
bootstrapServers = "cnt7-naya-cdh63:9092"
flights_topic = c.topic8
#news_information_topic = 'kafka-news-information-topic'

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

#df_kafka_flights = df_kafka_flights.withColumn('date_today', time.strftime("%m_%d_%y"))

df_kafka_flights = df_kafka_flights.withColumn("date_today", lit(time.strftime("%d_%m_%Y")))


#print(df_kafka)

#### Join All The DataFrames ######

#target_df = df_kafka_flights.join(df_kafka_news, "chat_id")



print(df_kafka_flights)

#df_kafka_flights = target_df

target_df = df_kafka_flights

#target_df.show

################### write to S3 #######################

def send_to_s3(target_df):
    #### Creating today's date and current timestamp in order to dynamically generate directory and file name in S3 bucket ####
    today = date.today()
    current_date = today.strftime("%d_%m_%Y")

    def format_time():
        t = datetime.datetime.now()
        s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
        return s[:-3]

    new_ts = format_time()
    current_timestamp = new_ts.split()[1].replace(":", "_").split(".")[0]

    #### Converting Dataframe to JSON ####
    json_df = pd.DataFrame(data=target_df)
    json_df = json_df.to_json()

    #with jsonlines.open(json_df, "w") as s3_writer:
    #    s3_writer.write_all(json_for_s3)

    #data = json.load(json_df)

    json_object = json.loads(json_df)

    json_object = json.dumps(json_object)

    json_object = json.loads(json_object)



    #### Creating S3 session using boto3 ####
    s3 = boto3.client("s3", \
                      region_name='us-east-1', \
                      aws_access_key_id='AKIAUILWXC7IWAPFX3DW', \
                      aws_secret_access_key='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')

    #### Uploading JSON File To S3 Bucket By Date Partition ####
    #s3.put_object(Bucket='travelappicationbucket',  Body=(bytes(json.dumps(json_object).encode('UTF-8'))), Key=f'request/{current_date}/{current_timestamp}.json')

    s3.put_object(Bucket='travelappicationbucket', Body=(bytes(json.dumps(json_object).encode('UTF-8'))),Key=f'request/{current_date}/{current_timestamp}.json')


#################################################################################


#### Spark Action ###
df_kafka_flights \
    .writeStream \
    .foreach(send_to_s3)\
    .outputMode("append") \
    .start() \
    .awaitTermination()
df_kafka_flights.show(truncate=False)

#### Spark Action ###
#target_df \
#    .writeStream \
#    .foreach(write_df_mongo_enriched_data)\
#    .outputMode("append") \
#    .start() \
#    .awaitTermination()
#target_df.show(truncate=False)


#print(post)
