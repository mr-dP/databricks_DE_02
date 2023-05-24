# Databricks notebook source
# COPY INTO

# SQL command that allow user to load data from a file location into a Delta table.
# It loads data idempotently and incrementally.
#     Each time you run the command it will only load the new files from the source location while the files that have been loaded before are simply skipped.

# COMMAND ----------

# COPY INTO my_table
# FROM '/path/to/files'
# FILEFORMAT = <format>
# FORMAT_OPTIONS (<format options>)
# COPY_OPTIONS (<copy options>);

# COMMAND ----------

# COPY INTO my_table
# FROM
#   '/path/to/files'
# FILEFORMAT = CSV
# FORMAT_OPTIONS ('delimiter' = '|', 'header' = 'true')
# COPY_OPTIONS ('mergeSchema' = 'true');

# In the COPY_OPTIONS, we are specifying that the schema can be evolved according to the incoming data

# COMMAND ----------



# COMMAND ----------

# AUTO LOADER

# It use Structured Streaming in Spark to efficiently process new data files as they arrive in a storage location
# You can use AUTO LOADER to load billions of files into a table
# AUTO LOADER can scale to support near real-time ingestion of millions of files per hour

# AUTO LOADER use checkpointing to track the ingestion process and store metadata of the discovered files
# AUTO LOADER ensure that the data files are processed exatcly once
# In case of failure, AUTO LOADER can resume from where it left off

# COMMAND ----------

# spark.readStream
#           .format("cloudFiles")
#           .option("cloudFiles.format", <source_format>)
#           .load("/path/to.files")
#       .writeStream
#           .option("checkpointLocation", <checkpoint_directory>)
#           .table(<table_name>)

# COMMAND ----------

# Auto Loader can automatically infer the schema of your data
# It can detect any update to the fields of the source dataset
# To avoid this inference cost at every startup of your stream, the inferred schema can be stored to a location to be used later.

# spark.readStream
#           .format("cloudFiles")
#           .option("cloudFiles.format", <source_format>)
#           .option("cloudFiles.schemaLocation", <schema_directory>)
#           .load("/path/to.files")
#       .writeStream
#           .option("checkpointLocation", <checkpoint_directory>)
#           .option("mergeSchema", "true")
#           .table(<table_name>)

# COMMAND ----------

# COPY INTO
# ---------
# Thousands of files
# Less efficient at scale


# AUTO LOADER
# -----------
# Millions of files
# Efficient at scale (Auto Loader can split the processing into multiple batches)

# COMMAND ----------



# COMMAND ----------

files = dbutils.fs.ls("dbfs:/FileStore/demo-datasets/bookstore/orders-raw")
display(files)

# COMMAND ----------

spark.readStream    \
        .format("cloudFiles")   \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.schemaLocation", "dbfs:/FileStore/demo/orders_checkpoint")  \
        .load("dbfs:/FileStore/demo-datasets/bookstore/orders-raw") \
    .writeStream    \
        .option("checkpointLocation", "dbfs:/FileStore/demo/orders_checkpoint") \
        .table("orders_updates")

# Auto Loader is a streaming query since it is using Spark Structured Streaming to load data incrementally
# This query will be conitnuously active and as soon as new data arrives in our data source it will be processed and loaded into our target table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   orders_updates;

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/FileStore/demo-datasets/bookstore/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates;
# MAGIC
# MAGIC -- A new table version is indicated for each streaming update

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates;

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/demo/orders_checkpoint", recurse=True)

# COMMAND ----------


