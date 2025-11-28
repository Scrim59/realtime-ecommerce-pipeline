# 📊 Real-Time E-Commerce Pipeline

**Kafka → Spark Structured Streaming → PostgreSQL → Metabase**

End-to-end real-time data engineering pipeline processing synthetic
e-commerce orders:\
**data generation → ingestion → transformation → storage → analytics.**

------------------------------------------------------------------------

## 🚀 Architecture Overview

    +------------------+      +------------------+      +------------------------+
    |  Python Generator|  --> |      Kafka       | -->  |   Spark Structured     |
    |  (Kafka Producer)|      |  (Streams Topic) |      |      Streaming         |
    +------------------+      +------------------+      +------------------------+
                                                               |
                                                               v
                                                      +------------------+
                                                      |    PostgreSQL    |
                                                      +------------------+
                                                               |
                                                               v
                                                      +------------------+
                                                      |     Metabase     |
                                                      +------------------+

------------------------------------------------------------------------

## 🧰 Tech Stack

**Python**, Faker, Kafka Producer\
**Apache Kafka**, Zookeeper\
**Spark Structured Streaming** (with UPSERT logic via JDBC)\
**PostgreSQL** (OLTP sink)\
**Metabase** (real-time dashboard)\
**Docker Compose**

------------------------------------------------------------------------

## 📦 Project Structure

    realtime-ecommerce-pipeline/
    │
    ├── generator/          # Kafka producer generating fake orders
	├── kafka/              # Kafka & Zookeeper docker-compose config
    ├── spark/              # Spark Structured Streaming transformation job
    ├── database/           # PostgreSQL init + Docker config
    ├── dashboard/          # Metabase docker-compose
    ├── README.md
    └── ...

------------------------------------------------------------------------

## 🔧 How to Run Locally

### 1️⃣ Start infrastructure (Kafka, Zookeeper, PostgreSQL)

``` bash
docker compose -f docker-compose.yml up -d
```

### 2️⃣ Start the order generator

``` bash
cd generator
python generate_orders.py
```

### 3️⃣ Start Spark Streaming

``` bash
cd spark
python stream_orders.py
```

### 4️⃣ Launch Metabase

``` bash
cd dashboard
docker compose up -d
```

Metabase → http://localhost:3000

------------------------------------------------------------------------

## 📈 Dashboard (Example)

Real-time dashboard built in Metabase, powered by continuously updated
PostgreSQL tables.

> 📌 
> `dashboard/dashboard.png`

![Dashboard](dashboard/dashboard.png)

Contains:
- Orders per hour
- Revenue by category
- AOV (today)
- Returning customer rate
- Orders by payment method
- Orders by channel
- Orders by country (map)
- Price vs quantity scatter

------------------------------------------------------------------------

## 🔥 Features

✔ Real-time event ingestion using Kafka\
✔ Nested JSON schema with `customer` + `items[]`\
✔ Spark flattening, exploding, and transformations\
✔ UPSERT logic (customers, orders, order_items)\
✔ PostgreSQL as streaming sink\
✔ Metabase dashboard updating in near-real-time\
✔ Fully reproducible with Docker

------------------------------------------------------------------------

## 📬 What This Project Demonstrates

-   Modern streaming architecture (Kafka → Spark → SQL)\
-   Building a real real-time data pipeline from scratch\
-   JSON normalization and deduplication challenges\
-   End-to-end ELT for analytics\
-   Hands-on SQL, Python, Spark, and data modeling

Everything spins up **locally in a few minutes**.

------------------------------------------------------------------------

## 👤 Author

**Michał Lipa**
