# Databricks notebook source
# Each commit to the table is written as a JSON file in the Dela Log sub-directory
# The JSON file contains the list of actions performed, like adding or removing data files from the table

# COMMAND ----------

# In order to resolce the current state of the table, Spark needs to process many tiny, inefficient JSON files

# COMMAND ----------

# To avoid this issue, Databricks automatically creates Parquet checkpoint files every 10 commits
# This accelarates the resolution of the current table state.
# This checkpoint files save the entire state of the table at a point in time in native parquet format that is quick and easy for Spark to read.
    # So rather than processing all the intermediate JSON files, Spark can skip ahead to the most recent checkpoint file
# and when new commits are added, Spark only has to perform incremental processing of the newly added JSON files from this starting point

# COMMAND ----------

# In transaction log, Delta Lake captures also statistics for each added data file
# these statistics indicate per file the total number of records in the file.
    # In addition to several statistics captured by default on the first 32 columns that appear in the table
        # These inlclude the minimum and maximum value in each column
        # And the number of null values as well for each of these columns

# Delta Lake file statistics will always be leveraged by Delta Lake for file skipping
    # For example, if you are querying the total number of records in a table, Delta will not calculate the counts by scanning all data files. Instead it will leverage these fiel statistics to generate the query result.

# COMMAND ----------

# Nested fields are taken into account when determining the first 32 columns.
    # For example, if your first 4 fields are STRUCTS with 8 nested fields each, they will total to the 32 columns.

# Delta Lake file statisics are generally uninformative for String fields with very high cardinality, such as free text fields like product review and user messages
# Calculating statistics on such a freeform text fields can be time consuming
    # So you need to emit these fields from statistic collection by setting them later in the schema after the first 32 columns

# COMMAND ----------

# How long the transaction lof files are kept?

# Running the VACUUM command does not delete them
    # VACUUM command only delete the data files

# Delta Log files are automatically cleaned up by Databricks
    # Each time a checkpoint is written, Databricks automatically cleans up log entries older than the log retention interval
    # The default for this log retention duration is 30 days

# By default, you can time travel to a Delta Table only up to 40 days old
    # You can change this duration by setting the table property "delta.logRetentionDuration"

# COMMAND ----------



# COMMAND ----------

# Delta Lake File Statistics are recorded in the transaction log files
# Transaction log files are located in the _delta_log directory within the table location

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bronze/_delta_log")
display(files)

# COMMAND ----------

# Let us query one of these JSON files directly with Spark
spark.read.json(
    "dbfs:/user/hive/warehouse/bronze/_delta_log/00000000000000000001.json"
).display()

# In the "add" column we see the list of added files in this commit. Delta Lake File Statistics are accessible in this "add" column
    # These statistics are limited to the first 32 columns in the table
# Delta Lake File Statistics are captured per data file

# COMMAND ----------

# MAGIC %sql
# MAGIC -- File Statistics can be leveraged directly. For example, when querying the total number of records in a table
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   bronze;
# MAGIC
# MAGIC -- Even if you have a huge table, you will notice that querying its count is super fast
# MAGIC   -- delta Lake does not need to calculate the count by scanning all data files. Instead, it will use the information stored in the transaction log to retrun the query results

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bronze/_delta_log")
display(files)

# COMMAND ----------

# The checkpoint file condenses all of the add and remove instructions and valid metadata into a single file

# COMMAND ----------


