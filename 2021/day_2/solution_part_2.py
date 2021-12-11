from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lag, monotonically_increasing_id, split, when
from pyspark.sql.functions import sum as sum_spark
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Advent of Code 2021") \
    .getOrCreate()

df: DataFrame = spark.read.csv("2021/day_2/puzzle_input")

df_with_contribs = df.select(
  col("_c0").alias("raw_command"),
  split(col("_c0"), " ").alias("command_parts")
).select(
  col("raw_command"),
  col("command_parts").getItem(0).alias("command"),
  col("command_parts").getItem(1).cast("integer").alias("argument")
).select(
  "*",
  when(
    col("command") == "forward",
    col("argument")
  ).otherwise(0).alias("horizontal_contribution"),
  when(
    col("command") == "down",
    col("argument")
  ).when(
    col("command") == "up",
    -col("argument")
  ).otherwise(0).alias("aim_contribution")
).select(
  "*",
  sum_spark("aim_contribution")
    .over(Window.orderBy(monotonically_increasing_id()))
    .alias("cumulative_aim")
).select(
  "*",
  (col("cumulative_aim")*col("horizontal_contribution")).alias("depth_contribution")
)

df_with_contribs.show()

df_with_contribs.agg(
  sum_spark("horizontal_contribution"),
  sum_spark("depth_contribution")
).show()
