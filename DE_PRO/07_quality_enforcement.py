# Databricks notebook source
# We are going to see how to add check contraints to Delta Tables to ensure the quality of our data

# Table constraints apply boolean filters to columns, and prevent data violating these constraints from being written

# COMMAND ----------

# We can define constraints on exisitng Delta Tables using "ALTER TABLE _ ADD CONSTRAINT" command

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE
# MAGIC   orders_silver
# MAGIC ADD
# MAGIC   CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01');

# COMMAND ----------

# Table constraints are listed under the properties of the extended TABLE DESCRIPTION

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver;
# MAGIC
# MAGIC -- Table Properties    [delta.constraints.timestamp_within_range=order_timestamp >= '2020-01-01',delta.minReaderVersion=1,delta.minWriterVersion=3]

# COMMAND ----------

# Now, let us see what happens when we attempt to insert new records where one of them violate the tables constraint

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   orders_silver
# MAGIC VALUES
# MAGIC   (
# MAGIC     '1',
# MAGIC     '2022-02-01 00:00:00.000',
# MAGIC     'C00001',
# MAGIC     0,
# MAGIC     0,
# MAGIC     NULL
# MAGIC   ),(
# MAGIC     '2',
# MAGIC     '2019-05-01 00:00:00.000',
# MAGIC     'C00001',
# MAGIC     0,
# MAGIC     0,
# MAGIC     NULL
# MAGIC   ),(
# MAGIC     '3',
# MAGIC     '2023-01-01 00:00:00.000',
# MAGIC     'C00001',
# MAGIC     0,
# MAGIC     0,
# MAGIC     NULL
# MAGIC   );
# MAGIC
# MAGIC -- The write operation failed because of the Contraint violation
# MAGIC -- com.databricks.sql.transaction.tahoe.schema.DeltaInvariantViolationException: CHECK constraint timestamp_within_range (order_timestamp >= '2020-01-01') violated by row with values:

# COMMAND ----------

# ACID guarantees in Delta Lake ensure that all transactions are Atomic, i.e., they will either succeed of fail completely
# So, none of the records above got inserted even the ones that do not violate the constraint

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   orders_silver
# MAGIC WHERE
# MAGIC   order_id IN ('1', '2', '3');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE
# MAGIC   orders_silver
# MAGIC ADD
# MAGIC   CONSTRAINT valid_quantity CHECK (quantity > 0);
# MAGIC
# MAGIC -- AnalysisException: 24 rows in spark_catalog.default.orders_silver violate the new CHECK constraint (quantity > 0)
# MAGIC
# MAGIC -- "ADD CONSTRAINT" command verifies that all existing rows satisfy the contraint before adding it to the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver;
# MAGIC
# MAGIC -- Table Properties    You[delta.constraints.timestamp_within_range=order_timestamp >= '2020-01-01',delta.minReaderVersion=1,delta.minWriterVersion=3]

# COMMAND ----------

# So, you must ensure that no data violating the contraint is already in the table prior to defining the constraint

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   orders_silver
# MAGIC WHERE
# MAGIC   quantity <= 0;

# COMMAND ----------

# How do we deal with this?
# We could, for example, manually delete the bad records and then set the CHECK contraint. Or, set the CHECK constraint before processing data from our bronze table.
# However, as we saw with the above timestamp constraint "timestamp_within_range", if a batch of data contains records that violate the constraints, the job will fail and throw an error.
# If our goal is to exclude bad records but keep streaming jobs running, we will need a different solution.

# COMMAND ----------

# We could separate such bad records into a quarantine table.
# Or, simply filter them out before writing the data into the table.

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (
    spark.readStream.table("bronze")
    .filter("topic = 'orders'")
    .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
    .select("v.*")
    .filter("quantity > 0")
    .writeStream.option(
        "checkpointLocation", "dbfs:/FileStore/demo_pro/checkpoints/orders_silver"
    )
    .trigger(availableNow=True)
    .table("orders_silver")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If we need to remove a constraint from a table, we use DROP CONSTRAINT command
# MAGIC
# MAGIC ALTER TABLE
# MAGIC   orders_silver DROP CONSTRAINT timestamp_within_range;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver;
# MAGIC
# MAGIC -- Table Properties       [delta.minReaderVersion=1,delta.minWriterVersion=3]

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_silver;

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/demo_pro/checkpoints/orders_silver", True)

# COMMAND ----------


