from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, split, trim, udf

spark = SparkSession \
    .builder \
    .appName("Day 2") \
    .getOrCreate()

df: DataFrame = spark.read.csv("2020/day_2/puzzle_input")

df_unpacked: DataFrame = df.select(
  split(col("_c0"), ":").alias("password_parts")
).select(
  col("password_parts").getItem(0).alias("policy"),
  trim(col("password_parts").getItem(1)).alias("password")
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
def letter_at_index(password, index):
    if len(password) < index:
      return "."
    else:
      return password[index-1]



meet_policy = df_unpacked.select(
  "*",
  letter_at_index(
    col("password"),
    col("min_appearances")
  ).alias("letter_at_first_index"),
  letter_at_index(
    col("password"),
    col("max_appearances")
  ).alias("letter_at_second_index")
).select(
  "*",
  (col("letter") == col("letter_at_first_index")).cast("integer").alias("match_first"),
  (col("letter") == col("letter_at_second_index")).cast("integer").alias("match_second")
).where(
  "match_first + match_second = 1"
)

meet_policy.show()

print(meet_policy.count())
