
raw_path = "s3://media-retail-314159/raw/media_type/"
staged_path = "s3://media-retail-314159/staged/media_type/"

df = spark.read.parquet(raw_path)


if 'load_date' in df.columns:
    df = df.drop('load_date')

# Write Delta table
df.write.format("delta") \
    .mode("overwrite") \
    .save(staged_path)

# register table in Databricks catalog
# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS staged_media_type
#     USING DELTA
#     LOCATION '{staged_path}'
# """)