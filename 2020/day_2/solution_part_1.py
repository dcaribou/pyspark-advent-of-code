from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, split, udf

spark = SparkSession \
    .builder \
    .appName("Day 2") \
    .getOrCreate()

df: DataFrame = spark.read.csv("2020/day_2/puzzle_input")

df_unpacked: DataFrame = df.select(
  split(col("_c0"), ":").alias("password_parts")
).select(
  col("password_parts").getItem(0).alias("policy"),
  col("password_parts").getItem(1).alias("password")
).select(
 split(col("policy"), " ").alias("policy_parts"),
 col("password")
).select(
  col("policy_parts").getItem(0).alias("limits"),
  col("policy_parts").getItem(1).alias("letter"),
  col("password")
).select(
  split(col("limits"), "-").alias("limits_parts"),
  col("letter"),
  col("password")
).select(
  col("limits_parts").getItem(0).alias("min_appearances").cast("integer"),
  col("limits_parts").getItem(1).alias("max_appearances").cast("integer"),
  col("letter"),
  col("password")
)

@udf
def count_letter(password, letter):
    return password.count(letter)

meet_policy = df_unpacked.select(
  "*",
  count_letter("password", "letter").alias("appearances").cast("integer"),
).where(
  "appearances <= max_appearances and appearances >= min_appearances"
)

meet_policy.show()

print(meet_policy.count())
