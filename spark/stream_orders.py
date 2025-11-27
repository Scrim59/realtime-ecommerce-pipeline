from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType
)

# ---- SPARK SESSION ----
spark = (
    SparkSession.builder
    .appName("RealtimeOrders")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2"
    )
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---- SCHEMA BASED ON YOUR JSON ----
customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("full_name", StringType()),
    StructField("email", StringType()),
    StructField("country", StringType()),
    StructField("device", StringType()),
    StructField("segment", StringType()),
    StructField("age", IntegerType())
])

item_schema = ArrayType(
    StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("line_total", DoubleType())
    ])
)

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("created_at", StringType()),
    StructField("currency", StringType()),
    StructField("status", StringType()),
    StructField("channel", StringType()),
    StructField("payment_method", StringType()),
    StructField("total_amount", DoubleType()),
    StructField("customer", customer_schema),
    StructField("items", item_schema)
])

# ---- READ FROM KAFKA ----
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) as json_str")
       .select(F.from_json("json_str", order_schema).alias("data"))
       .select("data.*")
       .withColumn("created_at", F.to_timestamp("created_at"))
)

# ---- EXPLODE ITEMS ----
exploded = (
    parsed
    .withColumn("item", F.explode("items"))
    .select(
        "order_id",
        "created_at",
        "currency",
        "status",
        "channel",
        "payment_method",
        "total_amount",

        F.col("customer.customer_id"),
        F.col("customer.full_name"),
        F.col("customer.email"),
        F.col("customer.country").alias("customer_country"),
        F.col("customer.device"),
        F.col("customer.segment"),
        F.col("customer.age"),

        F.col("item.product_id"),
        F.col("item.product_name"),
        F.col("item.category"),
        F.col("item.quantity"),
        F.col("item.unit_price"),
        F.col("item.line_total")
    )
)

# ---- SAMPLE AGGREGATION ----
agg = (
    exploded
    .withWatermark("created_at", "2 minutes")
    .groupBy(
        F.window("created_at", "1 minute"),
        F.col("category")
    )
    .agg(
        F.sum("line_total").alias("revenue"),
        F.approx_count_distinct("order_id").alias("unique_orders"),
        F.sum("quantity").alias("total_items")
    )
)

# ---- OUTPUT TO CONSOLE ----
query = (
    agg.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()