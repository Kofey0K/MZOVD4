from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.streaming.checkpointLocation", "task1/checkpoints")

schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("data1", StringType(), nullable=True),
    StructField("data2", StringType(), nullable=True),
    StructField("numeric_data", DoubleType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True)
])


static_data = spark.read.json("stream/data_stream1.json")
static_data.show()
stream_data = spark.readStream.format("json").schema(schema).load("stream")
joined_data = stream_data.join(static_data, on="id", how="left")
query = joined_data.coalesce(1).writeStream.format("json").option("path", "task1/result").outputMode("append").start()
query.awaitTermination()
