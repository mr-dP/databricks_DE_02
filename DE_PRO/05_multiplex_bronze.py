# Databricks notebook source
files = dbutils.fs.ls("dbfs:/FileStore/demo-datasets/DE-Pro/bookstore/kafka-raw")
display(files)

# We will use Auto-Loader to read the current file in this directory and detect new files as they arrive in order to ingest them into the multiplex bronze table.

# COMMAND ----------

df_raw = spark.read.json(
    "dbfs:/FileStore/demo-datasets/DE-Pro/bookstore/kafka-raw/01.json"
)

df_raw.display()

# Here we see full Kafka schema including the "topic"; "key" and "value" columns that are encoded in binary
# The value column represents the actual data sent as JSON format
# In addition, the "timestamp" column represent the time at which the Producer appended the records to a Kafka partition at a given "offset"

# COMMAND ----------

# Let us write a function to incrementally process this data from the source directory to the bronze table

from pyspark.sql import functions as F

def process_bronze():

    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (
        spark.readStream.format(
            "cloudFiles"
        )  # we start by configuring the Stream to use Auto Loader by specifying the "cloudFiles" format
        .option(
            "cloudFiles.format", "json"
        )  # Then we configure Auto Loader to use the "json" format
        .schema(schema)  # Then we provide the schema description
        .load("dbfs:/FileStore/demo-datasets/DE-Pro/bookstore/kafka-raw")
        .withColumn("timestamp", (F.col("timestamp") / 1000).cast("timestamp"))
        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
        .writeStream.option(
            "checkpointLocation", "dbfs:/FileStore/demo_pro/checkpoints/bronze"
        )
        .option(
            "mergeSchema", "true"
        )  # we are using the "mergeSchema" option to leverage the schema evolution functionality of Auto Loader
        .partitionBy(
            "topic", "year_month"
        )  # we parition the table by the "topic" and the "year_month" fields
        .trigger(availableNow=True)
        .table("bronze")
    )

    query.awaitTermination()


# "mergeSchema" will automatically evolve the schema of the table when new fields are detected in the input JSON files
# Since we are using the "availableNow" trigger option, out query is executed in a batch mode. It will process all the available data and then stop on its own

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze")
batch_df.display()

# In PySpark, you can easily create a DataFrame from a registerd table using the "spark.table()" funtion.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT(topic)
# MAGIC FROM
# MAGIC   bronze;

# COMMAND ----------

# Let us copy a new data file into our source directory
bookstore.load_new_data()
# This function is present in /Repos/databricks_DE_02/databricks_DE_02/DE_PRO/00_copy_datasets

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/FileStore/demo-datasets/DE-Pro/bookstore/kafka-raw")
display(files)

# COMMAND ----------

# Now, we can call our funtion to process this new batch of data
process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   bronze;

# COMMAND ----------


