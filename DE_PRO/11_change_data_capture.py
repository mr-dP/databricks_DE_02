# Databricks notebook source
# Change Data Capture (CDC) refers to the process of identifying and capturing changes made to data in the data source, and then delivering those changes to the target

# Those changes could be:
    # New records to be inserted from the source to the target
    # Updated records in the source that need to be reflected in the target
    # Deleted records in the source that must be deleted in the target

# COMMAND ----------

# Changes are logged at the source as events that contain both the data of the records along with metadata information
# These metadata indicate whether the specified record was inserted, updated or deleted. In addition, a version number or a timestamp indicating the orderin which changes happened

# Such a CDC feed could be received from the source as a data stream or simply in JSON files

# COMMAND ----------

# In Delta Lake, you can process CDC feed using the "MERGE INTO" command
# "MERGE INTO" command allows you to merge a set of updates, insertions and deletions based on a source table into a target DELTA Table

    # MERGE INTO target_table AS t
    # USING source_updates s
    #     ON t._key_field = s.key_field
    # WHEN MATCHED
    #     AND t.sequence_field < s.sequnce_field
    #     THEN
    #         UPDATE
    #         SET *
    # WHEN MATCHED
    #     AND s.operation_field = "DELETE"
    #     THEN
    #         DELETE
    # WHEN NOT MATCHED
    #     THEN
    #         INSERT *;

# COMMAND ----------

# Merge Limitation
# ----------------

# MERGE operation cannot be performed if multiple source rows matched and attempted to modify the same taregt row in the DELTA Table
    # If your CDC feed has multiple updates for the same key, this will generate an exception
    # To avoid this error you need to ensure that you are merging only the most recent changes
    # This can be achieved using the "rank()" window function
        # rank().over(Window)
        # This function assigns a rank number for each row within a window
        # A "Window" is a group of recors having the same partitioning key, and sorted by an ordering column in descending order. So, the most recent record for each key will have the rank "1"
        # Now we can filter to keep only records having rank equal "1" and merge them into our target table using "MERGE INTO" command

# COMMAND ----------



# COMMAND ----------

# We will create our "customers_silver" table
# The data in the "customer" topic contains complete row output from a Change Data Capture feed. The changes captured are either insert, update or delete

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import from_json, col

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time TIMESTAMP"

customers_df = (
    spark.table("bronze")
    .filter("topic = 'customers'")
    .select(from_json(col("value").cast("string"), schema).alias("v"))
    .select("v.*")
    .filter(
        col("row_status").isin(["insert", "update"])
    )  # we will process only insert and update in this notebook
)

display(customers_df)

# Multiple updates could be received for the same record, but with different "row_time". In such a case, we need to ensure applying the most recent change into our target table
# "dropDuplicates()" fucntion can be used to remove exact duplicates. However, here the problem is different since records are not identical for the same primary key

# COMMAND ----------

# The solution to keep only the most recent changes is to use the "rank()" window function
# This ranking function assigns a rank number for each row within a window
# A window is a group of ordered records haing the same partition key

from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank

window = Window.partitionBy("customer_id").orderBy(col("row_time").desc())

ranked_df = (
    customers_df.withColumn("rank", rank().over(window))
    .filter("rank = 1")
    .drop("rank")
)

display(ranked_df)

# we got only the newest entry for each unique "customer_id"

# COMMAND ----------

# Let us apply the above logic to a streaming read

ranked_df = (
    spark.readStream.table("bronze")
    .filter("topic = 'customers'")
    .select(from_json(col("value").cast("string"), schema).alias("v"))
    .select("v.*")
    .filter(col("row_status").isin(["insert", "update"]))
    .withColumn("rank", rank().over(window))
    .filter("rank = 1")
    .drop("rank")
)

ranked_df.display()

# AnalysisException: Non-time-based windows are not supported on streaming DataFrames/Datasets;
# Such a window operation is not supported on streaming DataFrames

# COMMAND ----------

# To avoid the above restriction we can use "foreachBatch()" logic
# Inside the streaming micro-batch process, we can interact with out data using batch syntax instead of streaming syntax

# Here we simply process the records of each batch before merging them into the target table

from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):

    # we start by computing the newest entries based on our window and store them in a temporary view called "ranked_updates"
    window = Window.partitionBy("customer_id").orderBy(col("row_time").desc())

    microBatchDF.filter(col("row_status").isin(["insert", "update"])).withColumn(
        "rank", rank().over(window)
    ).filter("rank == 1").drop("rank").createOrReplaceTempView("ranked_updates")

    # We merge these "ranked_updates" into our "customer" table based on the "customer_id" key
    # If the key already exists in the table, we update the record. And if the key does not exist, we insert the new record
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
            ON c.customer_id = r.customer_id
        WHEN MATCHED
            AND c.row_time < r.row_time
            THEN
                UPDATE
                SET *
        WHEN NOT MATCHED
            THEN
                INSERT *
    """
    # If we were interested in applying delete changes also, we could simply add another condition for this in our MERGE statement
            # WHEN MATCHED
            # AND r.row_status = "delete"
            # THEN
            #     DELETE

    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let us create our customers target table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver (
# MAGIC   customer_id STRING,
# MAGIC   email STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   gender STRING,
# MAGIC   street STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   row_time TIMESTAMP
# MAGIC );

# COMMAND ----------

# In the above query we are adding country name instead of country_code received in our customers data
# For this we will perform a join with the country lookup table

df_country_lookup = spark.read.json(
    "dbfs:/FileStore/demo-datasets/DE-Pro/bookstore/country_lookup"
)

display(df_country_lookup)

# COMMAND ----------

# We can write our streaming query

from pyspark.sql.functions import broadcast

query = (
    spark.readStream.table("bronze")
    .filter("topic = 'customers'")
    .select(from_json(col("value").cast("string"), schema).alias("v"))
    .select("v.*")
    .join(
        broadcast(df_country_lookup), col("country_code") == col("code"), "inner"
    )  # here we enrich our customer data by performing a join with the country lookup table. we suggest using "boradcast" join with this small lookup table
    .writeStream.foreachBatch(
        batch_upsert
    )  # we use "foreachBatch()" to merge the newest changes
    .option("checkpointLocation", "dbfs:/FileStore/demo/checkpoints/customers_silver")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# Broadcast Join is an optimization techniqu where the smaller DataFrame will be sent to all executor nodes in the cluster
# To allow Boradcast Join you just need to mark which DataFrame is small enough for broadcasting using the "broadcast()" function. this gives a hint to Spark that this DataFrame can fit in memory on all executors

# COMMAND ----------

# Now the customers table should have only one record for each unique ID

count = spark.table("customers_silver").count()
expected_count = (
    spark.table("customers_silver").select("customer_id").distinct().count()
)

assert count == expected_count
print("Unit test passed.")

# We are using "assert" statement to verify if the table count meets our expected distinct count.
# Assertions are boolean experessions that check if a statement is "true" or "false".
# Ther are used in Unit Tests to check if certain assumptions remain true while you are developing your code.

# The total number of records in our table equal to the unique number of customer_ids

# COMMAND ----------


