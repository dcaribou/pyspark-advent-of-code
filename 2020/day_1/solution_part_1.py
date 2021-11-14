from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("Day 1") \
    .getOrCreate()

df: DataFrame = spark.read.csv("2020/day_1/puzzle_input").select(
    col("_c0").alias("number")
)

sum_2020 = df.crossJoin(
    df.select(col("number").alias("number_other"))
).where(
    "number + number_other = '2020'"
)

sum_2020.select(
    sum_2020["number"] * sum_2020["number_other"]
).show()

