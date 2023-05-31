# Databricks notebook source
# We will see how to incrementally process delete requests and propagating deletes thorugh the lakehouse

# COMMAND ----------

# Delete requests are also known as requests to be forgotten
# they require deleting user data that represent Personally Identifiable Information (PII) such as the name and the email of the user

# COMMAND ----------

# In our case, we have 2 tables that contains PII data, which are "customers" and "customers_orders" silver tables

# COMMAND ----------

# We set up a table called "delete_requests" to track users requests to be forgotten
# These requests are recieved in the bronze table as CDC feed of type "delete" in the "customers" topic
# While it is possible to process deletes at the same time as inserts and updates CDC Feed, the fines around right to be forgotten requests may require a separate process

from pyspark.sql.functions import col, from_json, date_add, lit

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time TIMESTAMP"

(
    spark.readStream.table("bronze")
    .filter("topic = 'customers'")
    .select(from_json(col("value").cast("string"), schema).alias("v"))
    .select(
        "v.*", col("v.row_time").alias("request_timestamp")
    )  # here we indicate the time at which the delete was requested
    .filter("row_status = 'delete'")
    .select(
        "customer_id",
        "request_timestamp",
        date_add("request_timestamp", 30).alias(
            "deadline"
        ),  # we add 30 days as a deadline to ensure compliance
        lit("requested").alias(
            "status"
        ),  # we provide a field that indicates the current processing status of the request
    )
    .writeStream.outputMode("append")
    .option(
        "checkpointLocation", "dbfs:/FileStore/demo_pro/checkpoints/delete_requests"
    )
    .trigger(availableNow=True)
    .table("delete_requests")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delete_requests;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now we can start deleting user records from the customers table based on the customer_id
# MAGIC
# MAGIC DELETE
# MAGIC FROM customers_silver
# MAGIC WHERE customer_id IN (
# MAGIC 		SELECT customer_id
# MAGIC 		FROM delete_requests
# MAGIC 		WHERE STATUS = 'requested'
# MAGIC 		);

# COMMAND ----------

# We enabled CDF on the customers table previously. So, we can leverage CDF data as an incremental records of data changes to propagate deleted to downstream tables
# Here we configure an incremental read of all change events committed to the customers table

deleteDF = (
    spark.readStream.format("delta")
    .option("readChangeDataFeed", "true")
    .option("startingVersion", 2)
    .table("customers_silver")
)

# COMMAND ----------

# Now we define a function to be called with foreachBatch() in order to process the delete events


def process_deletes(microBatchDF, batchId):

    microBatchDF.filter("_change_type = 'delete'").createOrReplaceTempView("deletes")

    # Here we commit deletes to other tables containing PII data in our pipeline.
    # In our case, it is only one table, which is the "customers_orders" silver table.
    microBatchDF._jdf.sparkSession().sql(
        """
                            DELETE
                            FROM customers_orders
                            WHERE customer_id IN (
                                    SELECT customer_id
                                    FROM deletes
                                    )
                                         """
    )

    # We perform an update batch to the "delete_requests" table to change the status of the request to "deleted"
    microBatchDF._jdf.sparkSession().sql(
        """
                            MERGE INTO delete_requests r
                            USING deletes d
                                ON d.customer_id = r.customer_id
                            WHEN MATCHED
                                THEN
                                    UPDATE
                                    SET STATUS = 'deleted'
                                         """
    )

# COMMAND ----------

# Lastly, we run a trigger "availableNow" batch to propagate the deletes using our "foreachBatch()" logic
(
    deleteDF.writeStream.foreachBatch(process_deletes)
    .option("checkpointLocation", "dbfs:/FileStore/demo_pro/checkpoints/deletes2")
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delete_requests;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can also review what happened in our "customers_orders" table
# MAGIC
# MAGIC DESCRIBE HISTORY customers_orders;
# MAGIC -- The log shows a DELETE opration performed on the table

# COMMAND ----------

# Are deletes fully committed?

# NO, not exactly.
# Because of how Delta Lake tables TIME TRAVEL and CDF features are implemented, deleted values are still present in older versions of the data
# With Delta Lake, deleting data does not delete the data files from the table directory. Instead, it creates a copy of the affected files without these deleted records

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This query will show only the records deleted in the previous version of the "customers_orders" table
# MAGIC
# MAGIC SELECT *
# MAGIC FROM customers_orders@v6
# MAGIC
# MAGIC EXCEPT
# MAGIC
# MAGIC SELECT *
# MAGIC FROM customers_orders;
# MAGIC
# MAGIC -- The deleted records are still there

# COMMAND ----------

# Similarly, while deletes are already committed to the customers table, this information is still available within the CDC Feed

df = (
    spark.read.option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .table("customers_silver")
    .filter("_change_type = 'delete'")
)

display(df)

# COMMAND ----------

# To fully commit these deletes, you need to run VACUUM commands on these tables
