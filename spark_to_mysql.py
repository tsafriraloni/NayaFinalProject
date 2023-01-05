import os
from pyspark.sql import types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dateutil.parser import parse
import mysql.connector as mc

### Creating a Conection Between Spark And Kafka ####
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

#================== connection between  spark and kafka=======================================#
spark = SparkSession.builder.appName("From_Kafka_To_parquet") .getOrCreate()

#=========================================== ReadStream from kafka===========================#
df_kafka_flights = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", 'cnt7-naya-cdh63:9092')\
    .option("subscribe", 'From_Kafka_To_Spark_MYSQL')\
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

#
# #============================================================================================#
# #====Cleanse money column (convert to  float by removing '$' from the data) =================#
def normalizeMoney(num):
    if num is not None:
        return float(num.replace('$', ''))
    else:
        return 0.0
# #============================================================================================#
# #======= Create Date object from Date in String type=========================================#
# #============================================================================================#
def stringToDateTime(num):
    if num is not None:
        return parse(num)
    else:
        return None
dateTimeUdf = udf(stringToDateTime, T.TimestampType())
moneyUdf = udf(normalizeMoney, T.DoubleType())
# #============================================================================================#
# #====2 : Update data types of money and date time columns====================================#
# #============================================================================================#
df_kafka_flights  = df_kafka_flights\
    .withColumn("price", moneyUdf(df_kafka_flights.price)) \
    .withColumn("date_departure",dateTimeUdf(df_kafka_flights.date_departure))
# #============================================================================================#
# #==== 3: Add date columns from timestamp=====================================================#
# #============================================================================================#
df_kafka_flights = df_kafka_flights.withColumn('FlightStartDT', df_kafka_flights['date_departure'].cast('date'))
df_kafka_flights = df_kafka_flights.withColumn('date_departure_return', df_kafka_flights['date_departure_return'].cast('date'))
#
# #============================================================================================#
# #==== 5: Extract Day, Month and Year from timestamp column===================================#
# #============================================================================================#
hourd = F.udf(lambda x: x.hourd, T.IntegerType())
minuted = F.udf(lambda x: x.minuted, T.IntegerType())
secondd = F.udf(lambda x: x.secondd, T.IntegerType())
#
df_kafka_flights = df_kafka_flights.withColumn('FlightStartDT', df_kafka_flights['date_departure'].cast('date'))
df_kafka_flights = df_kafka_flights.withColumn('date_departure_return', df_kafka_flights['date_departure_return'].cast('date'))
#
#
#
df_kafka_flights = df_kafka_flights\
.withColumn("hourd", hour(df_kafka_flights["FlightStartDT"])) \
    .withColumn("FlightStartYear", year(df_kafka_flights["FlightStartDT"])) \
    .withColumn("minuted", minute(df_kafka_flights["FlightStartDT"])) \
    .withColumn("secondd", second(df_kafka_flights["FlightStartDT"]))\
    .withColumn("dayofweek", dayofweek(df_kafka_flights["FlightStartDT"])) \
    .withColumn("week_day_full", date_format(F.col("FlightStartDT"), "EEEE"))

df_kafka_flights = df_kafka_flights\
    .drop("FlightStartDT","FlightEndDT")
#df_kafka_flights.printSchema()


def procss_row(events):
    # connector to mysql
    mysql_conn = mc.connect(
        user='naya',
        password='NayaPass1!',
        host='localhost',
        port=3306,
        autocommit=True,  # <--
        database='MyFlightdb')

    insert_statement = """
    INSERT INTO MyFlightdb.flights(
        location_arrival,date_departure,location_departure,date_departure_return,price,      
        currencyCode,carrier,totalTripDurationInHours,hourd,       
        FlightStartYear,minuted,secondd,dayofweek,week_day_full)
        VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
               ; """
#
    #print(insert_statement)
#
    mysql_cursor = mysql_conn.cursor()
    sql = insert_statement.format(
        events["location_arrival"], events["date_departure"], events["location_departure"],
        events["date_departure_return"], events["price"], events["currencyCode"],
        events["carrier"], events["totalTripDurationInHours"], events["hourd"],
        events["FlightStartYear"], events["minuted"], events["secondd"],
        events["dayofweek"], events["week_day_full"]
    )
    #print(sql)
    mysql_conn.commit()
    mysql_cursor.execute(sql)
    mysql_cursor.close()
    pass


Insert_To_MYSQL_DB = df_kafka_flights \
    .writeStream \
    .foreach(procss_row) \
    .outputMode("append") \
    .start()
Insert_To_MYSQL_DB.awaitTermination()