# Databricks notebook source
# We are going to see how to use CDF data to porpagate changes to downstream tables
# We will create a new silver table called "customers_orders" by joining 2 stereams - the "orders" table with the CDF data of the "customers" table

# COMMAND ----------

# we will create a function to upsert ranked_updates into our new table
# we will use the same logic we used before for CDC feed processing

from pyspark.sql.functions import from_json, col, rank
from pyspark.sql.window import Window


def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("order_id", "customer_id").orderBy(
        col("_commit_timestamp").desc() # we are using the "_commit_timestamp" column to sort the records in the window
    )

    microBatchDF.filter(
        col("_change_type").isin(["insert", "update_postimage"])    # in order to get the new changes, we are filtering for 2 change types
    ).withColumn("rank", rank().over(window)).filter("rank == 1").drop(
        "rank", "_change_type", "_commit_version"
    ).withColumnRenamed(
        "_commit_timestamp", "processed_timestamp"
    ).createOrReplaceTempView(
        "ranked_updates"
    )

    query = """
        MERGE INTO customers_orders c
        USING ranked_updates r
            ON c.order_id = r.order_id
                AND c.customer_id = r.customer_id
        WHEN MATCHED
            AND c.processed_timestamp < r.processed_timestamp
            THEN
                UPDATE
                SET *
        WHEN NOT MATCHED
            THEN
                INSERT *
    """

    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We will define our new table
# MAGIC CREATE TABLE IF NOT EXISTS customers_orders (
# MAGIC   order_id STRING,
# MAGIC   order_timestamp TIMESTAMP,
# MAGIC   customer_id STRING,
# MAGIC   quantity BIGINT,
# MAGIC   total BIGINT,
# MAGIC   books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>,
# MAGIC   email STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   gender STRING,
# MAGIC   street STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   row_time TIMESTAMP,
# MAGIC   processed_timestamp TIMESTAMP
# MAGIC );

# COMMAND ----------

# Now we can define our streaming query to write our new table "customers_orders"
# here we are performing a join operation between 2 streaming tables


def process_customers_orders():
    orders_df = spark.readStream.table(
        "orders_silver"
    )  # we are reading the "orders" table as a streaming source

    cdf_customers_df = (
        spark.readStream.option("readChangeData", "true")
        .option("startingVersion", 2)
        .table("customers_silver")
    )  # we are reading the "customers" CDF data as a streaming source

    query = (
        orders_df.join(cdf_customers_df, ["customer_id"], "inner")
        .writeStream.foreachBatch(batch_upsert)
        .option(
            "checkpointLocation",
            "dbfs:/FileStore/demo_pro/checkpoints/customers_orders",
        )
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()


process_customers_orders()

# COMMAND ----------

# when performing stream-stream join, Spark buffers past input as streaming state for both input streams, so that it can match every future input with past inputs and accordingly generate the joined results
# Similary to streaming de-duplication. we can limit the state using Watermarks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customers_orders;

# COMMAND ----------

# We can now land a new data file and process it to see how all our changes will be propagated through our pipeline

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_orders_silver()
bookstore.porcess_customers_silver()

process_customers_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customers_orders;

# COMMAND ----------


