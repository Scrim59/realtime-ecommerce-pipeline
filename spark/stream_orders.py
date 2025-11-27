from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, ArrayType
)

# ===============================================
#   UPSERT INTO POSTGRES
# ===============================================

def save_to_postgres(df, batch_id):
    import psycopg2

    print(f"==== BATCH {batch_id} START ====")

    conn = psycopg2.connect(
        dbname="ecommerce",
        user="admin",
        password="admin",
        host="localhost"
    )
    cur = conn.cursor()

    # ---------------------------
    # 1) CUSTOMERS UPSERT
    # ---------------------------
    customers = (
        df.select(
            "customer_id", "full_name", "email",
            "customer_country", "device", "segment", "age"
        )
        .dropDuplicates(["customer_id"])
        .collect()
    )

    for c in customers:
        cur.execute("""
            INSERT INTO customers (customer_id, full_name, email, country, device, segment, age)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                email = EXCLUDED.email,
                country = EXCLUDED.country,
                device = EXCLUDED.device,
                segment = EXCLUDED.segment,
                age = EXCLUDED.age;
        """, (
            c["customer_id"], c["full_name"], c["email"],
            c["customer_country"], c["device"], c["segment"], c["age"]
        ))

    print("✔ customers upserted")

    # ---------------------------
    # 2) ORDERS UPSERT
    # ---------------------------
    orders = (
        df.select(
            "order_id", "customer_id", "created_at", "currency",
            "status", "channel", "payment_method", "total_amount"
        )
        .dropDuplicates(["order_id"])
        .collect()
    )

    for o in orders:
        cur.execute("""
            INSERT INTO orders (order_id, customer_id, created_at, currency,
                                status, channel, payment_method, total_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                created_at = EXCLUDED.created_at,
                currency = EXCLUDED.currency,
                status = EXCLUDED.status,
                channel = EXCLUDED.channel,
                payment_method = EXCLUDED.payment_method,
                total_amount = EXCLUDED.total_amount;
        """, (
            o["order_id"], o["customer_id"], o["created_at"], o["currency"],
            o["status"], o["channel"], o["payment_method"], o["total_amount"]
        ))

    print("✔ orders upserted")

    # ---------------------------
    # 3) ORDER ITEMS UPSERT
    # ---------------------------
    items = (
        df.select(
            "order_id", "product_id", "product_name",
            "category", "quantity", "unit_price", "line_total"
        ).collect()
    )

    for it in items:
        cur.execute("""
            INSERT INTO order_items (order_id, product_id, product_name,
                                     category, quantity, unit_price, line_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                line_total = EXCLUDED.line_total;
        """, (
            it["order_id"], it["product_id"], it["product_name"],
            it["category"], it["quantity"], it["unit_price"], it["line_total"]
        ))

    print("✔ order_items upserted")

    conn.commit()
    cur.close()
    conn.close()

    print(f"==== BATCH {batch_id} END ====")


# ===============================================
#   SPARK SESSION
# ===============================================

spark = (
    SparkSession.builder
    .appName("RealtimeOrders")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.postgresql:postgresql:42.7.4"
    )
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===============================================
#   SCHEMAS
# ===============================================

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

# ===============================================
#   READ FROM KAFKA
# ===============================================

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

# ===============================================
#   FLATTEN STRUCTS
# ===============================================

flat = (
    parsed
    .withColumn("customer_id",      F.col("customer.customer_id"))
    .withColumn("full_name",        F.col("customer.full_name"))
    .withColumn("email",            F.col("customer.email"))
    .withColumn("customer_country", F.col("customer.country"))
    .withColumn("device",           F.col("customer.device"))
    .withColumn("segment",          F.col("customer.segment"))
    .withColumn("age",              F.col("customer.age"))
    .withColumn("item",             F.explode("items"))
    .withColumn("product_id",       F.col("item.product_id"))
    .withColumn("product_name",     F.col("item.product_name"))
    .withColumn("category",         F.col("item.category"))
    .withColumn("quantity",         F.col("item.quantity"))
    .withColumn("unit_price",       F.col("item.unit_price"))
    .withColumn("line_total",       F.col("item.line_total"))
    .drop("customer", "items", "item")
)

# ===============================================
#   STREAM TO POSTGRES (UPSERT)
# ===============================================

query = (
    flat.writeStream
    .foreachBatch(save_to_postgres)
    .outputMode("append")
    .start()
)

query.awaitTermination()