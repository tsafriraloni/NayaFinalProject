import pymongo
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType
from pyspark.sql import SparkSession

### Creating a Conection Between Spark And Kafka ####
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
bootstrapServers = "cnt7-naya-cdh63:9092"
#topic = "kafka-flights-topic"
topic = 'kafka-flights-requests-topic'
####  Creating Spark Session ####
spark = SparkSession\
        .builder\
        .appName("flights")\
        .getOrCreate()

#### ReadStream From Kafka Topic ####
df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option("subscribe", topic)\
    .load()



#### Creating a Schema for Spark Structured Streaming ####
schema = StructType() \
    .add("chat_id", StringType())\
    .add("first_name", StringType())\
    .add("last_name", StringType())\
    .add("class_type", StringType())\
    .add("location_arrival", StringType())\
    .add("date_departure", StringType())\
    .add("location_departure", StringType())\
    .add("date_departure_return", StringType())\
    .add("price", StringType())\
    .add("currencyCode", StringType())\
    .add("carrier", StringType())\
    .add("totalTripDurationInHours", StringType())


### Change Json To Dataframe With Schema ####
df_kafka = df_kafka.select(col("value").cast("string"))\
    .select(from_json(col("value"), schema).alias("value"))\
    .select("value.*")

#### Adding Calculated Columns To Spark Data Frame ####
df_kafka = df_kafka.withColumn("current_ts", current_timestamp().cast('string'))
df_kafka= df_kafka.withColumn("is_active", lit(1))

#print(df_kafka)


#### Defining A Function To Send Dataframe To MongoDB ####
def write_df_mongo_enriched_data(target_df):

    mogodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = mogodb_client["Flights_DB"]
    #mycol = mydb["Flights_Requests"]
    mycol = mydb["Flights_Offered"]
    #x = mycol.delete_many({})
    post = {
      #  "request_id": request_id,
        "chat_id": target_df.chat_id,
        "first_name": target_df.first_name,
        "last_name": target_df.last_name,
        "class_type": target_df.class_type,
        "location_arrival": target_df.location_arrival,
        "date_departure": target_df.date_departure,
        "date_departure_return": target_df.date_departure_return,
        "price": target_df.price,
        "currencyCode": target_df.currencyCode,
        "carrier": target_df.carrier,
        "totalTripDurationInHours": target_df.totalTripDurationInHours,
        "current_ts": target_df.current_ts,
        "is_active": target_df.is_active
    }

    mycol.insert_one(post)
    print('item inserted')
#### Spark Action ###
df_kafka \
    .writeStream \
    .foreach(write_df_mongo_enriched_data)\
    .outputMode("append") \
    .start() \
    .awaitTermination()
df_kafka.show(truncate=False)

#print(post)
