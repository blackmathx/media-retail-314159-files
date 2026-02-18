from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number


spark = SparkSession.builder \
    .appName("Genre Raw to Staged Dedup") \
    .getOrCreate()

raw_path = "s3://media-retail-314159/raw/genre/*/*.parquet"
staged_path = "s3://media-retail-314159/staged/genre/"

# Read raw data, all load_date partitions
df_raw = spark.read.parquet(raw_path)

# Deduplicate and keep latest updated_at for genre_id
window_spec = Window.partitionBy("genre_id") \
                    .orderBy(col("updated_at").desc())

df_deduped = df_raw \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")



# Overwrite staged
df_deduped.write \
    .format("delta") \
    .mode("overwrite") \
    .save(staged_path)

spark.stop()








