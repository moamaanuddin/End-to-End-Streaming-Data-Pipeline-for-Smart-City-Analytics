from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json
from config import configuration
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,TimestampType
def main():
    spark=SparkSession.builder.appName("SparkCity")\
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026")\
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key",configuration.get("AWS_ACCESS_KEY"))\
        .config("spark.hadoop.fs.s3a.secret.key",configuration.get("AWS_SECRET"))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    vehicle_schema=StructType([
        StructField("id",StringType(),True),
        StructField("device_id",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("location",StringType(),True),
        StructField("speed",DoubleType(),True),
        StructField("direction",StringType(),True),
        StructField("make",StringType(),True),
        StructField("model",StringType(),True),
        StructField("year",StringType(),True),
        StructField("fuel_type",StringType(),True),
        StructField("snapshot",StringType(),True)
    ])

    gps_schema=StructType([
        StructField("id",StringType(),True),
        StructField("device_id",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("speed",DoubleType(),True),
        StructField("direction",StringType(),True),
        StructField("vehicle_type",StringType(),True)
        
    ])
    
    traffic_camera_schema=StructType([
        StructField("id",StringType(),True),
        StructField("device_id",StringType(),True),
        StructField("camera_id",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("snapshot",StringType(),True)
     
        ])
    
    weather_schema=StructType([
        StructField("id",StringType(),True),
        StructField("device_id",StringType(),True),
        StructField("location",StringType(),True),
        StructField("temperature",DoubleType(),True),
        StructField("weather_condition",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("precipitation",DoubleType(),True),
        StructField("wind_speed",DoubleType(),True),
        StructField("humidity",DoubleType(),True),
        StructField("air_quality_index",DoubleType(),True)

    ])

    emergency_incident_schema=StructType([
        StructField("id",StringType(),True),
        StructField("device_id",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("location",StringType(),True),
        StructField("type",StringType(),True),
        StructField("incident_id",StringType(),True),
        StructField("status",StringType(),True),
        StructField("description",StringType(),True)
    ])
    def read_kafka_topic(topic_name,schema):
        return (spark.readStream.format("kafka")\
                .option("kafka.bootstrap.servers","broker:29092")
                .option("subscribe",topic_name)\
                .option("startingOffsets","latest")\
                .load()\
                .selectExpr("CAST(value as string)")\
                .select(from_json("value",schema).alias("data"))\
                .select("data.*")\
                .withWatermark("timestamp","2 minute"))


    def stream_writer(input:DataFrame,checkpointfolder,output):
        return(
        input.writeStream.format("parquet")\
        .option("checkpointLocation",checkpointfolder)\
        .option("path",output)\
        .outputMode("append")\
        .start())


    vehicle_df=read_kafka_topic('VEHICLE_TOPIC',vehicle_schema).alias("vehicle")
    gps_df=read_kafka_topic('GPS_TOPIC',gps_schema).alias("gps")
    traffic_camera_df=read_kafka_topic('TRAFFIC_TOPIC',traffic_camera_schema).alias("camera")
    weather_df=read_kafka_topic('WEATHER_TOPIC',weather_schema).alias("weather")
    emergency_incident_df=read_kafka_topic('EMERGENCY_TOPIC',emergency_incident_schema).alias("incident")


    query_1=stream_writer(vehicle_df,"s3a://spark-city-streaming-data-buck/checkpoint/vehicle_data","s3a://spark-city-streaming-data-buck/data/vehicle_data")
    query_2=stream_writer(gps_df,"s3a://spark-city-streaming-data-buck/checkpoint/gps_data","s3a://spark-city-streaming-data-buck/data/gps_data")
    query_3=stream_writer(traffic_camera_df,"s3a://spark-city-streaming-data-buck/checkpoint/traffic_camera_data","s3a://spark-city-streaming-data-buck/data/traffic_camera_data")
    query_4=stream_writer(weather_df,"s3a://spark-city-streaming-data-buck/checkpoint/weather_data","s3a://spark-city-streaming-data-buck/data/weather_data")
    query_5=stream_writer(emergency_incident_df,"s3a://spark-city-streaming-data-buck/checkpoint/emergency_incident_data","s3a://spark-city-streaming-data-buck/data/emergency_incident_data")
    query_5.awaitTermination()
if __name__=="__main__":
    main()