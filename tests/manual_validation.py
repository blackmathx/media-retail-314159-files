from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("media-retail-show-select").getOrCreate()

STAGED_BASE = "s3://media-retail-314159/staged"

album_df = spark.read.format("delta").load(f"{STAGED_BASE}/album")
artist_df = spark.read.format("delta").load(f"{STAGED_BASE}/artist")
customer_df = spark.read.format("delta").load(f"{STAGED_BASE}/customer")
genre_df = spark.read.format("delta").load(f"{STAGED_BASE}/genre")
invoice_df = spark.read.format("delta").load(f"{STAGED_BASE}/invoice")
invoice_line_df = spark.read.format("delta").load(f"{STAGED_BASE}/invoice_line")
track_df = spark.read.format("delta").load(f"{STAGED_BASE}/track")


invoice_window = Window.orderBy(col("invoice_id"))

dim_invoice = (
    invoice_df
    .join(invoice_line_df())
    .dropDuplicates(["invoice_id"])
    .withColumn("invoice_sk", row_number().over(invoice_window))
    .select(
        col("invoice_sk"),
        col("invoice_id").alias("invoice_key"),
        col("customer_id"),
        col("billing_country"),
        col("billing_state"),
        col("total"),
        current_timestamp().alias("created_at")
    )
)

dim_track.show(n=20, truncate=False)
