from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as fn

TOPIC_NAME_CONS = "topic-weather"
BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

postgresql_host_name = "localhost"
postgresql_port_no = "5432"
postgresql_user_name = "harsha"
postgresql_password = "harsha"
postgresql_database_name = "event_message_db"
postgresql_driver = "org.postgresql.Driver"

db_properties = {}
db_properties['user'] = postgresql_user_name
db_properties['password'] = postgresql_password
db_properties['driver'] = postgresql_driver

def save_to_postgresql_table(current_df, epoc_id, postgresql_table_name):
    print("Inside save_to_postgresql_table function")
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing postgresql_table_name: " + postgresql_table_name)

    postgresql_jdbc_url = "jdbc:postgresql://" + postgresql_host_name + ":" + str(postgresql_port_no) + "/" + postgresql_database_name

    #Save the dataframe to the table.
    current_df.write.jdbc(url = postgresql_jdbc_url,
                          table = postgresql_table_name,
                          mode = 'append',
                          properties = db_properties)

    print("Exit out of save_to_postgresql_table function")

if __name__ == "__main__":
    print("Real-Time Streaming Data Pipeline Started ...")

    spark = SparkSession \
            .builder \
            .appName("Real-Time Streaming Data Pipeline") \
            .master("local[*]") \
            .config("spark.jars", "spark_dependency_jars/commons-pool2-2.8.1.jar,"
                                "spark_dependency_jars/postgresql-42.2.16.jar,"
                                "spark_dependency_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar,"
                                "spark_dependency_jars/kafka-clients-2.6.0.jar,"
                                "spark_dependency_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
            .config("spark.executor.extraClassPath", "spark_dependency_jars/commons-pool2-2.8.1.jar:"
                                                    "spark_dependency_jars/postgresql-42.2.16.jar:"
                                                    "spark_dependency_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:"
                                                    "spark_dependency_jars/kafka-clients-2.6.0.jar:"
                                                    "spark_dependency_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
            .config("spark.executor.extraLibrary", "spark_dependency_jars/commons-pool2-2.8.1.jar:"
                                                "spark_dependency_jars/postgresql-42.2.16.jar:"
                                                "spark_dependency_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:"
                                                "spark_dependency_jars/kafka-clients-2.6.0.jar:"
                                                "spark_dependency_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
            .config("spark.driver.extraClassPath", "spark_dependency_jars/commons-pool2-2.8.1.jar:"
                                                "spark_dependency_jars/postgresql-42.2.16.jar:"
                                                "spark_dependency_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:"
                                                "spark_dependency_jars/kafka-clients-2.6.0.jar:"
                                                "spark_dependency_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    weather_detail_df = spark \
                              .readStream \
                              .format("kafka") \
                              .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_CONS) \
                              .option("subscribe", TOPIC_NAME_CONS) \
                              .option("startingOffsets", "latest") \
                              .load()

    print("Printing Schema of weather_detail_df: ")
    weather_detail_df.printSchema()

    weather_detail_schema = StructType([
      StructField("event_datetime", TimestampType()),
      StructField("humidity", LongType()),
      StructField("pressure", LongType()),
      StructField("temperature", FloatType()),
      StructField("temperature_max", FloatType()),
      StructField("temperature_min", FloatType()),
      StructField("temperature_feels", FloatType()),
      StructField("city_name", StringType()),
      StructField("wind_deg", LongType()),
      StructField("wind_speed", FloatType()),
    ])

    weather_detail_df_1 = weather_detail_df.selectExpr("CAST(value AS STRING)")

    weather_detail_df_2 = weather_detail_df_1.select(from_json(col("value"), weather_detail_schema).alias("weather_detail"))

    weather_detail_df_3 = weather_detail_df_2.select("weather_detail.*")

    print("Printing Schema of weather_detail_df_1: ")
    weather_detail_df_3.printSchema()

    hdfs_weather_path = "hdfs://localhost:9000/weather_data/data"
    hdfs_weather_checkpoint_location_path = "hdfs://localhost:9000/weather_data/checkpoint"

    weather_detail_df_3.writeStream \
                       .trigger(processingTime='20 seconds') \
                       .format("json") \
                       .option("path", hdfs_weather_path) \
                       .option("checkpointLocation", hdfs_weather_checkpoint_location_path) \
                       .start()

    weather_detail_agg_df = weather_detail_df_3\
                            .groupby("event_datetime",
                                    "city_name")\
                            .agg(fn.avg('humidity').alias('humidity'),
                                 fn.avg('pressure').alias('pressure'),
                                 fn.avg('temperature').alias('temperature'),
                                 fn.avg('temperature_max').alias('temperature_max'),
                                 fn.avg('temperature_min').alias('temperature_min'),
                                 fn.avg('temperature_feels').alias('temperature_feels'),
                                 fn.avg('wind_deg').alias('wind_deg'),
                                 fn.avg('wind_speed').alias('wind_speed'))

    
    postgresql_table_name = "event_message_detail_tbl1"
    weather_detail_agg_df .writeStream \
                          .trigger(processingTime='60 seconds') \
                          .outputMode("update") \
                          .foreachBatch(lambda current_df, epoc_id: save_to_postgresql_table(current_df, epoc_id, postgresql_table_name)) \
                          .start()

    weather_detail_write_stream = weather_detail_agg_df .writeStream \
                                                        .trigger(processingTime='60 seconds') \
                                                        .outputMode("update") \
                                                        .option("truncate", "false")\
                                                        .format("console") \
                                                        .start()

    weather_detail_write_stream.awaitTermination()
    print("Real-Time Streaming Data Pipeline Completed.")