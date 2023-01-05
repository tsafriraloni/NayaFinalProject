import os
from pyspark.sql import types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import configuration as c

### Creating a Conection Between Spark And Kafka ####
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

#================== connection between  spark and kafka=======================================#

spark = SparkSession.builder.appName("From_Kafka_To_parquet") .getOrCreate()

#=========================================== ReadStream from kafka===========================#
df_kafka_flights = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", c.bootstrapServers)\
    .option("subscribe", c.topic4)\
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

#==============================================================================================
#==============================Create schema for create df from json=========================#
schema = T.StructType() \
    .add("location_arrival", T.StringType())\
    .add("date_departure", T.StringType())\
    .add("location_departure", T.StringType())\
    .add("date_departure_return", T.StringType())\
    .add("price", T.StringType())\
    .add("currencyCode", T.StringType())\
    .add("carrier", T.StringType())\
    .add("totalTripDurationInHours", T.StringType())
#==============================================================================================
#==========================change json to dataframe with schema==============================#

df_kafka_flights = df_kafka_flights.select(F.col("value").cast("string"))\
    .select(from_json(F.col("value"), schema).alias("value"))\
    .select("value.*")

#============================================================================================#
#======= Create Date object from Date in String type=========================================#
#============================================================================================#
def stringToDateTime(num):
    if num is not None:
        return parse(num)
    else:
        return None
dateTimeUdf = udf(stringToDateTime, T.TimestampType())


#============================================================================================#
#==== 3: Add date columns from timestamp=====================================================#
#============================================================================================#
df_kafka_flights = df_kafka_flights.withColumn('FlightStartDT', df_kafka_flights['date_departure'].cast('date'))
df_kafka_flights = df_kafka_flights.withColumn('FlightEndDT', df_kafka_flights['date_departure_return'].cast('date'))

#============================================================================================#
#==== 4: Extract Day, Month and Year from timestamp column===================================#
#============================================================================================#
hourd = F.udf(lambda x: x.hourd, T.IntegerType())
minuted = F.udf(lambda x: x.minuted, T.IntegerType())
secondd = F.udf(lambda x: x.secondd, T.IntegerType())

df_kafka_flights = df_kafka_flights\
    .withColumn("FlightStartDay", dayofmonth(df_kafka_flights["FlightStartDT"])) \
    .withColumn("FlightStartMonth", month(df_kafka_flights["FlightStartDT"]))\
    .withColumn("FlightStartYear", year(df_kafka_flights["FlightStartDT"]))

df_kafka_flights = df_kafka_flights\
    .withColumn("hourd", hour(df_kafka_flights["FlightStartDT"])) \
    .withColumn("minuted", minute(df_kafka_flights["FlightStartDT"])) \
    .withColumn("secondd", second(df_kafka_flights["FlightStartDT"]))\
    .withColumn("dayofweek", dayofweek(df_kafka_flights["FlightStartDT"])) \
    .withColumn("week_day_full", date_format(col("FlightStartDT"), "EEEE"))

## CREATE A CLEANED DATA-FRAME BY DROPPING SOME UN-NECESSARY COLUMNS
df_kafka_flights = df_kafka_flights\
    .drop('FlightStartDT','FlightEndDT')


df_kafka_flights.printSchema()


hdfs_query = df_kafka_flights\
    .writeStream\
    .format("parquet")\
    .option("path","hdfs://Cnt7-naya-cdh63:8020/user/Flight_Project/Data/")\
    .option("checkpointLocation","hdfs://Cnt7-naya-cdh63:8020/user/Flight_Project/Check_Point/")\
    .outputMode("append")\
    .start()

hdfs_query.awaitTermination()