from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

# refresh s3 cache
#spark.read.format("delta").option("recursiveFileLookup","true").load("s3://media-retail-314159/staged/media_type/")


spark = SparkSession.builder \
    .appName("Media Type Raw to Staged Dedup") \
    .getOrCreate()

raw_path = "s3://media-retail-314159/raw/media_type/*/*.parquet"
staged_path = "s3://media-retail-314159/staged/media_type/"

# Read raw data, all load_date partitions
df_raw = spark.read.parquet(raw_path)

# Deduplicate and keep latest updated_at for media_type_id
window_spec = Window.partitionBy("media_type_id") \
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










