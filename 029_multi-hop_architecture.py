# Databricks notebook source


# COMMAND ----------

# Multi-Hop Architechture usually consists of 3 layers - bronze, silver and gold


# Bronze table contains raw data ingested from various sources like JSON files, operational databases, or Kafka Streams

# Silver table privide more refined view of our data. For example, data can be cleaned and filtered at this level. And we can also join fields from various bronze tables to enrich our silver records.

# Gold table provide business level aggregation often used for reporting and dashboarding or even for machine learning


# With this architechture we incrementally improve the structure and the quality of data as it flow through each layer

# COMMAND ----------

# Benefits:
# --------

# Simple data model

# Enables incremental ETL

# Combine streaming and batch worlkloads in unified pipeline

# Can recreate your tables from raw data at any time

# COMMAND ----------



# COMMAND ----------

files = dbutils.fs.ls("dbfs:/FileStore/demo-datasets/bookstore/orders-raw")
display(files)

# COMMAND ----------

spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option(
    "cloudFiles.schemaLocation", "dbfs:/FileStore/demo/checkpoints/orders_raw"
).load("dbfs:/FileStore/demo-datasets/bookstore/orders-raw").createOrReplaceTempView(
    "orders_raw_temp"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     current_timestamp() AS arrival_time,
# MAGIC     input_file_name() AS source_file
# MAGIC   FROM
# MAGIC     orders_raw_temp
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   orders_tmp;

# COMMAND ----------

spark.table("orders_tmp").writeStream.format("delta").option(
    "checkpointLocation", "dbfs:/FileStore/demo/checkpoints/orders_bronze"
).outputMode("append").table("orders_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   orders_bronze;

# COMMAND ----------

spark.read.format("json").load(
    "dbfs:/FileStore/demo-datasets/bookstore/customers-json"
).createOrReplaceTempView("customer_lookup")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_lookup;

# COMMAND ----------

spark.readStream.table("orders_bronze").createOrReplaceTempView("orders_bronze_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     quantity,
# MAGIC     o.customer_id,
# MAGIC     c.profile :first_name AS f_name,
# MAGIC     c.profile :last_name AS l_name,
# MAGIC     CAST(
# MAGIC       from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp
# MAGIC     ) order_timestamp,
# MAGIC     books
# MAGIC   FROM
# MAGIC     orders_bronze_tmp o
# MAGIC     INNER JOIN customer_lookup c ON o.customer_id = c.customer_id
# MAGIC   WHERE
# MAGIC     quantity > 0
# MAGIC )

# COMMAND ----------

spark.table("orders_enriched_tmp").writeStream.format("delta").option(
    "checkpointLocation", "dbfs:/FileStore/demo/checkpoints/orders_silver"
).outputMode("append").table("orders_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   orders_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   orders_silver;

# COMMAND ----------

spark.readStream.table("orders_silver").createOrReplaceTempView("orders_silver_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     f_name,
# MAGIC     l_name,
# MAGIC     date_trunc("DD", order_timestamp) order_date,
# MAGIC     sum(quantity) books_count
# MAGIC   FROM
# MAGIC     orders_silver_tmp
# MAGIC   GROUP BY
# MAGIC     customer_id,
# MAGIC     f_name,
# MAGIC     l_name,
# MAGIC     date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

spark.table("daily_customer_books_tmp").writeStream.format("delta").outputMode(
    "complete"
).option(
    "checkpointLocation", "dbfs:/FileStore/demo/checkpoints/daily_customer_books"
).trigger(
    availableNow=True
).table(
    "daily_customer_books"
)

# COMMAND ----------

# Structured Streaming assumes data is only being appended in the upstream tables. Once a table is updated or overwritten, it is no longer valid for streaming.
# So, we cannot read a stream for the gold table "daily_customer_books"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   daily_customer_books;

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: ", s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


