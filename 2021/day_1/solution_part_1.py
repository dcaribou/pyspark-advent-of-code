from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lag, monotonically_increasing_id, when
from pyspark.sql.functions import sum as sum_spark
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Advent of Code 2021") \
    .getOrCreate()

df: DataFrame = spark.read.csv("2021/day_1/puzzle_input")

df_with_increases = df.select(
  col("_c0").cast("integer").alias("height")
).select(
  "*",
  lag(col("height"))
    .over(Window.orderBy(monotonically_increasing_id()))
    .alias("previous_height")
).select(
  "*",
  when(col("height") > col("previous_height"), 1).otherwise(0).alias("increased")
)

df_with_increases.agg(
  sum_spark(col("increased"))
).show()


