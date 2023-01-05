# ========================================================================================================= #
# ========================================Kafka Connections =============================================== #
# ========================================================================================================= #
bootstrapServers = "cnt7-naya-cdh63:9092"
#flights_requests_topic = "kafka-flights-requests-topic"
#news_information_topic = "kafka-news-information-topic"
topic1 = 'From_API_To_Kafka'
topic2 = 'From_Kafka_To_Hdfs_Archive_Json'
topic3 = 'From_Kafka_To_Spark_MYSQL'
topic4 = 'From_Kafka_To_Hdfs_Parquet'

#topic4 = 'kafka-flights-requests-topic'
# ========================================================================================================= #
# ========================================flight api =============================================== #
# ========================================================================================================= #
flights_api="https://priceline-com-provider.p.rapidapi.com/v1/flights/search"


# # ======== Format DataFrame to json file and Write it to HDFS  ==================== #
# kafka_to_hdfs_json_path = 'hdfs://Cnt7-naya-cdh63:8020/user/FlightProject/Hdfs_Archive/'
# kafka_to_hdfs_json_checkpoint_path = 'hdfs://Cnt7-naya-cdh63:8020/user/FlightProject/Hdfs_Archive/HdfsArchive.checkpoint/'

# ======== Format DataFrame to parquet file and Write it to HDFS  ==================== #
From_Kafka_To_Hdfs_Parquet_path = 'hdfs://cnt7-naya-cdh63:8020/user/hdfs/FlightProject/Data/'
From_Kafka_To_Hdfs_parquet_path_checkpointLocation = 'hdfs://cnt7-naya-cdh63:8020/user/hdfs/FlightProject/CheckPoint/'
# ========================================================================================================= #
# =================================sql stocks connection =============================================== #
# ========================================================================================================= #
mysql_host = 'localhost'
#mysql_host = '34.170.0.175'
mysql_port = 3306
mysql_database_name = 'MyFlightdb'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'
mysql_table_name = 'Flights'

# ========================================================================================================= #
# =================================/hive/ =============================================== #
host='Cnt7-naya-cdh63'
port=8020
user='hdfs'
#user='naya'

hdfs_host = 'Cnt7-naya-cdh63'
hdfs_owner ='hdfs'
hdfs_group='supergroup'
source_path = '/user/FlightProject/Flight_Parquet/'
# ====== Settings to HUE Connection ===================== #
hue_port = 8889
hue_username = 'hdfs'
hue_password = 'naya'
# ====== Settings to Hive Connection ===================== #
hdfs_host = 'Cnt7-naya-cdh63'
hdfs_port = 9870
hive_port = 10000
hive_username = 'hdfs'
hive_password = 'naya'
hive_mode = 'CUSTOM'
hive_database= 'flight_db'
# ====== Settings to impala Connection ===================== #
impala_host = 'Cnt7-naya-cdh63'
impala_port = 21050
impala_database = 'flight_hive_db'
impala_username = 'hdfs'
impala_password = 'naya'



