# Databricks notebook source
# We will create our 2 gold layer entities
# We will start by creating a view in the gold layer against our silver table "customers_orders"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Our view will simply contain some aggregations for daily sales per country
# MAGIC -- Here we are counting orders_count and books_count per country and order day
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS countries_state_vw AS (
# MAGIC   SELECT
# MAGIC     country,
# MAGIC     date_trunc("DD", order_timestamp) order_date,
# MAGIC     count(order_id) orders_count,
# MAGIC     sum(quantity) books_count
# MAGIC   FROM
# MAGIC     customers_orders
# MAGIC   GROUP BY
# MAGIC     country,
# MAGIC     date_trunc("DD", order_timestamp)
# MAGIC );

# COMMAND ----------

# A view is nothing but a SQL query against tables
# If you have a complex query with joins and sub-queries, each time you query the view, Delta have to scan and join files from multiple tables, which would be really costly

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_state_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_state_vw
# MAGIC WHERE
# MAGIC   country = "France";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let us re-run the above query and see how fast it is
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_state_vw;

# COMMAND ----------

# Even if you have a view with complex logic, re-executing the view will be super fast on the currently active cluster
# In fact, to save costs Databricks uses a feature called DELTA CACHING
# So, subsequent execution of queries will use cached results
# However, this result is not guaranteed to be persisted and is only cached for the currently active cluster

# COMMAND ----------

# In traditional databses, usually you can control cost associated with materializing results using Materialized Views"
# In Databricks, the concept of a Materialized View most closely maps to that of a gold table.
# Gold Tables help to cut down the potential cost and latency associated with complex ad-hoc queries

# COMMAND ----------

# Let us now create the Gold Table

# COMMAND ----------

# "authors_stats" will store summary statistics of sales per author
# It calculates the "orders_count" and the "avg_quantity" per author and per order_timestamp for each non-overlapping 5 minutes interval
# Similary to streaming de-duplication, we automatically handle late, out-of-order data and limit the state using watermarks
# Here we define a watermask of 10 minutes during which incremental state information is maintained for late arriving data

from pyspark.sql.functions import window, count, avg

query = (
    spark.readStream.table("books_sales")
    .withWatermark("order_timestamp", "10 minutes")
    .groupBy(window("order_timestamp", "5 minutes").alias("time"), "author")
    .agg(count("order_id").alias("orders_count"), avg("quantity").alias("avg_quantity"))
    .writeStream.option(
        "checkpointLocation", f"dbfs:/FileStore/demo_pro/checkpoints/author_stats"
    )
    .trigger(availableNow=True)
    .table("author_stats")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   author_stats;

# COMMAND ----------


