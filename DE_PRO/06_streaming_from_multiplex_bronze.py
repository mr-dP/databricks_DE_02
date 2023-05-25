# Databricks notebook source
# We are going to parse raw data from a single topic in our multiplex bronze table
# We will create the "orders" silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Before starting we need to cast our Kafka BINARY fields as STRINGs
# MAGIC SELECT
# MAGIC   cast(key AS STRING),
# MAGIC   cast(value AS STRING)
# MAGIC FROM
# MAGIC   bronze
# MAGIC LIMIT
# MAGIC   20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   v.*
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       from_json(
# MAGIC         cast(value AS STRING),
# MAGIC         "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
# MAGIC       ) v
# MAGIC     FROM
# MAGIC       bronze
# MAGIC     WHERE
# MAGIC       topic = "orders"
# MAGIC   );
# MAGIC
# MAGIC -- We can parse the "orders" data using "from_json()" function. For this we need to provde the schema description

# COMMAND ----------

# Let's convert the above logic into a Streaming read

# COMMAND ----------

spark.readStream.table("bronze").createOrReplaceTempView("bronze_tmp")

# First, we need to convert our Static table into a Streaming temporary view
# This allow us to write streaming queries with SparkSQL 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Next, we can update our above query to refer to the streaming teamporary view
# MAGIC SELECT
# MAGIC   v.*
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       from_json(
# MAGIC         cast(value AS STRING),
# MAGIC         "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
# MAGIC       ) v
# MAGIC     FROM
# MAGIC       bronze_tmp
# MAGIC     WHERE
# MAGIC       topic = "orders"
# MAGIC   );

# COMMAND ----------

# Let's define the above SQL logic in a temporary view to pass it back to Python

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW orders_silver_tmp AS (
# MAGIC   SELECT
# MAGIC     v.*
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         from_json(
# MAGIC           cast(value AS STRING),
# MAGIC           "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
# MAGIC         ) v
# MAGIC       FROM
# MAGIC         bronze_tmp
# MAGIC       WHERE
# MAGIC         topic = "orders"
# MAGIC     )
# MAGIC );
# MAGIC
# MAGIC -- This Temporary view is used as intermediary to capture the query we want to apply

# COMMAND ----------

# Now we can create our "orders" silver table

query = (
    spark.table("orders_silver_tmp")
    .writeStream.option(  # we use a streaming write to persist the result of our streaming temporary view to disk
        "checkpointLocation", "dbfs:/FileStore/demo_pro/checkpoints/orders_silver"
    )
    .trigger(availableNow=True)
    .table("orders_silver")
)

query.awaitTermination()

# COMMAND ----------

# We can express the entire SQL logic using PySpark DataFrame API

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (
    spark.readStream.table("bronze")
    .filter("topic = 'orders'")
    .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
    .select("v.*")
    .writeStream.option(
        "checkpointLocation", "dbfs:/FileStore/demo_pro/checkpoints/orders_silver"
    )
    .trigger(availableNow=True)
    .table("orders_silver")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   orders_silver;

# COMMAND ----------


