from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number


spark = SparkSession.builder \
    .appName("Track Raw to Staged Dedup") \
    .getOrCreate()

raw_path = "s3://media-retail-314159/raw/track/*/*.parquet"
staged_path = "s3://media-retail-314159/staged/track/"

# Read raw data, all load_date partitions
df_raw = spark.read.option("mergeSchema", "true").parquet(raw_path)

# Deduplicate and keep latest updated_at for track_id
window_spec = Window.partitionBy("track_id") \
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








