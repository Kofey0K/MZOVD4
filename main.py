from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ =="__main__":
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    schema = StructType([
        StructField("artist_name", StringType(), True),
        StructField("album_name", StringType(), True),
        StructField("track_number", DoubleType(), True),
        StructField("song_id", DoubleType(), True),
        StructField("song_name", StringType(), True),
        StructField("line_number", DoubleType(), True),
        StructField("section_name", StringType(), True),
        StructField("line", StringType(), True),
        StructField("section_artist", StringType(), True)
    ])

    lyrics_df = (
        spark
        .readStream
        .format("csv")
        .schema(schema)
        .option("header", True)
        .load("D:\\DZ_magistr\\MZOVD\\")
    )

    query = (
        lyrics_df
        .writeStream
        .format("console")
        .outputMode("append")
        .start()
    ).awaitTermination()

    spark.stop()

    wordCountDf = (
        lyrics_df
        .select(
            explode(split(col("artist_name"), "\\s+"))
            .alias("word")
        )
        .groupBy("word")
        .count()
    )

    query = (
        wordCountDf
        .writeStream
        .format("console")
        .outputMode("complete")
        .start()
    ).awaitTermination()

    spark.stop()