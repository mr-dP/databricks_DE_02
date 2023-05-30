# Databricks notebook source
# Partitioning is a strategy for optimizing query performance on large Delta Tables
# A Partition is composed of a subset of rows in a table that share the same value for a predefined subset of columns called the Partitioning Columns

# COMMAND ----------

# To use Partitioning, you deifned the set of partitioning columns when you create the table by including the PARTITIONED BY clause
            # CREATE TABLE my_table (
            #     id INT,
            #     name STRING,
            #     year INT,
            #     month INT
            #     ) PARTITIONED BY (year)

# When inserting rows into the table, Databricks will automatically create a data directory for each partition and dispatches rows into the appropriate directory
# Using partitions can speed up the queries against the table as well as data manipulation

# COMMAND ----------

# Partition Skipping
# ------------------

# When running a query that filters data on a partitioning column, partitions not matching the conditional statement will be skipped entirely
# Delta Lake also have several opertations like OPTIMIZE commands that can be applied at the partition level

# COMMAND ----------

# Multiple Partitioning Columns
# -----------------------------

# You can partition your table by more than one column

            # CREATE TABLE my_table (
            #     id INT,
            #     name STRING,
            #     year INT,
            #     month INT
            #     ) PARTITIONED BY (
            #     year,
            #     month
            #     )

# COMMAND ----------

# When chosing partitioning columns, it is good to consider the following:

    # How many values present in a column - Chose always low cardinality fields

    # The size of a partition
        # Partitions should be atleast 1 GB is size or larger depending on total table size
    
    # If records with a given value will continue to arrive indefinitely, we can use a date-time column for partitioning
        # This allows the partitions to be optimized and easily archived if necessary

# Columns representing measures of time and low cardinality fields that also used frequently in queries are good candidates for partitioning

# COMMAND ----------

# Because data files will be separated into different directories based on partition values, files cannot be combined or compated across these partition boundaries
    # This means that, small tables that use partitioning will increase the storage cost and total number of files to scan

# If most partitions in a tabled have less tha 1 GB of data, the table is likely over-partitoned

# The data that is over-partitioned or incorrectly partitioned will suffer greatly
    # It leads to slowdown for most general queries
    # It will require a full re-write of all data files to remedy

# COMMAND ----------

# Partitioning is also useful when removing data older than a certain age from the table
    # For example, you can decide to delete the previous year's data. In this case file deletion will be clearly along partition boundaries
                # DELETE FROM my_table
                # WHERE year < 2023

# In the same way data could be archived or backed up at partition boundaries to a cheaper storage tier. This drives a huge saving on a cloud storage

# But if you are using this table as a streaming source, remember that deleting data breaks the append-only requirement of streaming sources, which makes the table no more streambale
    # To avoid this, you need to use the "ignoreDeletes" option when streaming from this table
    # This option enables the streaming processing from Delta Tables with partition deletes
                # spark.readStream
                #     .option("ignoreDeletes", True)
                #     .table("my_table")

# COMMAND ----------

# The deletion of files will not actually occur until you run "VACUUM" command on the table

# COMMAND ----------

# We had partitioned our bronze table "bronze" by 2 fields - topic, year_month
# Let us explore how the data is stored in our table directory

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First we need to know the location of our table
# MAGIC
# MAGIC DESCRIBE TABLE EXTENDED bronze;

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bronze")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bronze/topic=customers/")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bronze/topic=customers/year_month=2021-12/")
display(files)

# COMMAND ----------


