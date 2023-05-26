# Databricks notebook source
# Many source systems like Kafka introduce duplicate records
# We will see how to eliminate the duplicate records while working with Structured Streaming

# COMMAND ----------

# We will apply deduplication at the silver level rather than the bronze level
# The Bronze table should retain a history of the true state of our streaming source
# This prevents potential data loss due to applying aggressive equality enforcement and pre-processing at the initial ingestion, and also helps minimizing latencies for data ingestion

# COMMAND ----------

# Let us identify the current number of records in our "orders" topic of the bronze table
spark.read.table("bronze").filter("topic = 'orders'").count()

# COMMAND ----------

# With the static data, we can simply use "dropDuplicates()" function to eliminate duplicate records

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (
    spark.read.table("bronze")
    .filter("topic = 'orders'")
    .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
    .select("v.*")
    .dropDuplicates(["order_id", "order_timestamp"])
    .count()
)

print(batch_total)

# COMMAND ----------

# In Spark Structured Streaming we can also use "dropDuplicates()" function
# Structured Streaming can track state information for the unique keys in the data. This ensures that duplicate records do not exist within or between micro-batches
# However, over time this state information will scale to represent all history. We can limit the amount of the state to be maintained by using WATERMARKING
# WATERMARKING allows to only track state information for a window of time in which we expect records could be delayed

# Hewe we define a WATERMARK of 30 seconds

deduped_df = (
    spark.readStream.table("bronze")
    .filter("topic = 'orders'")
    .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
    .select("v.*")
    .withWatermark("order_timestamp", "30 seconds")
    .dropDuplicates(["order_id", "order_timestamp"])
)

# COMMAND ----------

# When dealing with Streaming deduplication, there is another level of complexity compared to static data
# As each micro-batch is processed we need also to ensure that records to be inserted are not already in the target table
# We can achieve this using insert-only merge. This operation is ideal for deduplication
# It defines logic to match on unique keys and only insert those records for keys that do not exist in the table

# Here we define a function to be called at each new micro-batch processing
# In this function, we first store the records we inserted into a temporary view called "orders_microbatch". We then use this temporary view in our insert-only merge query
# Lastly we execute this query using "spark.sql()" function
# However in this particular case, the Spark Session cannot be accessed from within the micro-batch process. Instead we can access the local Spark Session from the micro-batch DataFrame
# The clusters with runtime version below 10.5, the syntax to access the local Spark Session is slightly different:
#                   microBatchDF._jdf_sparkSession().sql(sql_query)


def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")

    sql_query = """
        MERGE INTO orders_silver a
        USING orders_microbatch b
        ON a.order_id = b.order_id AND a.order_timestamp = b.order_timestamp
        WHEN NOT MATCHED THEN INSERT *
    """

    # spark.sql(sql_query)
    microBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- And since we are using SQL for writing to our Delta Table, we need to make sure that this table exists before we begin
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver (
# MAGIC   order_id STRING,
# MAGIC   order_timestamp TIMESTAMP,
# MAGIC   customer_id STRING,
# MAGIC   quantity BIGINT,
# MAGIC   total BIGINT,
# MAGIC   books ARRAY < STRUCT < book_id STRING, quantity BIGINT, subtotal BIGINT > >
# MAGIC );

# COMMAND ----------

# In order to code the upsert function in our stream, we need to use the "foreachBatch()" method
# This provides the option to execute customer data writing logic on each micro-batch of a streaming data
# In our case, this custom logic is the insert-only MERGE for deduplication

query = (
    deduped_df.writeStream.foreachBatch(upsert_data)
    .option("checkpointLocation", "dbfs:/FileStore/demo_pro/checkpoints/orders_silver")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

# Let us see the number of entries that have been processed into the "orders_silver" table

streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")

# COMMAND ----------


