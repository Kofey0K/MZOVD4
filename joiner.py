from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
import os

def is_valid_directory(path):
    return os.path.isdir(path)

spark = SparkSession.builder.appName("Minin").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.streaming.checkpointLocation", "task2/checkpoints")

schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("data1", StringType(), nullable=True),
    StructField("data2", StringType(), nullable=True),
    StructField("numeric_data", DoubleType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True)
])

stream_path = "stream"
stream_path2 = "stream2"
result_path = "task2/result"
checkpoint_path = "task2/checkpoints"

if is_valid_directory(stream_path) and is_valid_directory(stream_path2) and \
        is_valid_directory(result_path) and is_valid_directory(checkpoint_path):
    stream_data_1 = spark.readStream.format("json").schema(schema).load(stream_path)
    stream_data_2 = spark.readStream.format("json").schema(schema).load(stream_path2)

    united_data = stream_data_1.union(stream_data_2).withWatermark("generated_timestamp", "3 hours")

    query = united_data.coalesce(1).writeStream.format("json").option("path", result_path).outputMode(
        "append").start()
    query.awaitTermination()
else:
    print("Check whether the paths are correct")
