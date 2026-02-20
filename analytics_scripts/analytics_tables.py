from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("media-retail-analytics").getOrCreate()

STAGED_BASE = "s3://media-retail-314159/staged"
ANALYTICS_BASE = "s3://media-retail-314159/analytics"

# ----------------------------
# Read staged tables
# ----------------------------
album_df = spark.read.format("delta").load(f"{STAGED_BASE}/album")
artist_df = spark.read.format("delta").load(f"{STAGED_BASE}/artist")
customer_df = spark.read.format("delta").load(f"{STAGED_BASE}/customer")
genre_df = spark.read.format("delta").load(f"{STAGED_BASE}/genre")
invoice_df = spark.read.format("delta").load(f"{STAGED_BASE}/invoice")
invoice_line_df = spark.read.format("delta").load(f"{STAGED_BASE}/invoice_line")
track_df = spark.read.format("delta").load(f"{STAGED_BASE}/track")

# =====================================================
# DIMENSIONS WITH INCREASING INT SURROGATE KEYS
# =====================================================

# ----------------------------
# dim_customer
# ----------------------------
customer_window = Window.orderBy(col("customer_id"))

dim_customer = (
    customer_df
    .dropDuplicates(["customer_id"])
    .withColumn("customer_sk", row_number().over(customer_window))
    .select(
        col("customer_sk"),
        col("customer_id").alias("customer_key"),
        col("country"),
        col("state"),
        current_timestamp().alias("created_at")
    )
)

dim_customer.write.format("delta") \
    .mode("overwrite") \
    .save(f"{ANALYTICS_BASE}/dim_customer")


# ----------------------------
# dim_genre
# ----------------------------
genre_window = Window.orderBy(col("genre_id"))

dim_genre = (
    genre_df
    .dropDuplicates(["genre_id"])
    .withColumn("genre_sk", row_number().over(genre_window))
    .select(
        col("genre_sk"),
        col("genre_id").alias("genre_key"),
        col("name"),
        current_timestamp().alias("created_at")
    )
)

dim_genre.write.format("delta") \
    .mode("overwrite") \
    .save(f"{ANALYTICS_BASE}/dim_genre")


# ----------------------------
# dim_track
# ----------------------------
track_window = Window.orderBy(col("track_id"))

# Step 1: Join album to track to get album title and artist_id
track_with_album = track_df.join(
    album_df.select("album_id", col("title").alias("album"), "artist_id"),
    on="album_id",
    how="inner"
)

# Step 2: Join artist to get artist name
dim_track = track_with_album.join(
    artist_df.select("artist_id", col("name").alias("artist")),
    on="artist_id",
    how="left"
).dropDuplicates(["track_id"]) \
 .withColumn("track_sk", row_number().over(track_window)) \
 .select(
     col("track_sk"),
     col("track_id").alias("track_key"),
     col("name").alias("track"),
     col("album"),
     col("artist"),
     current_timestamp().alias("created_at")
 )


dim_track.write.format("delta") \
    .mode("overwrite") \
    .save(f"{ANALYTICS_BASE}/dim_track")

# ----------------------------
# dim_invoice
# ----------------------------
invoice_window = Window.orderBy(col("invoice_id"))

dim_invoice = (
    invoice_df
    .dropDuplicates(["invoice_id"])
    .withColumn("invoice_sk", row_number().over(invoice_window))
    .select(
        col("invoice_sk"),
        col("invoice_id").alias("invoice_key"),
        col("invoice_date"),
        current_timestamp().alias("created_at")
    )
)

dim_invoice.write.format("delta") \
    .mode("overwrite") \
    .save(f"{ANALYTICS_BASE}/dim_invoice")


# =====================================================
# FACT TABLE (using surrogate keys)
# =====================================================

fact_inventory = (
    invoice_line_df
    .join(dim_customer, "customer_id", "left")
    .join(dim_track, "track_id", "left")
    .select(
        col("inventory_line_id").alias("inventory_line_key"),
        col("customer_sk"),
        col("track_sk"),
        col("quantity"),
        col("unit_price"),
        (col("quantity") * col("unit_price")).alias("total_amount"),
        current_timestamp().alias("created_at")
    )
)

fact_inventory.write.format("delta") \
    .mode("overwrite") \
    .save(f"{ANALYTICS_BASE}/fact_inventory")

print("Analytics layer successfully rebuilt.")