# Databricks notebook source
# Change Data Feed (CDF) is a new feature built into Delta Lake that allows to automatically generate CDC feeds about Delta Lake tables

# CDF records row-level changes for all the data written into a Delta Table
    # these include the raw data along with metadata indicating whether the specidifed row was inserted, deleted or updated.

# COMMAND ----------

# CDF is used to prapagate incremental changes to downstream tables in a multi-hop architecture

# In he table_changes, CDF records the row data along with the change_type, indicating whether the specified row was inserted, updated or deleted, and the timestamp associated when the change was applied. In addition, it records the Delta Table version containing the change
    # Update operations in the table_changes have always 2 records present with pre-image and post-image change_type. This records the row values before and after the update, which can be used to evaluate the differences in the changes id needed

# COMMAND ----------

# We query the change data of a table starting from a specified table version
    # SELECT * FROM table_changes('table_name', 'start_version', [end_version])

# alternatively you can provide a timestamp for the start and end limits.
    # SELECT * FROM table_changes('table_name', 'start_timestamp', [end_timestamp])

# COMMAND ----------

# CDF is not enabled by default. You can explicitly enable it using one of the following methods:

# New tables
    # CREATE TABLE myTable (id INT, name STRING)
    # TBLPROPERTIES (delta.enableChangeDataFeed = true)

# Exisitng table
    # ALTER TABLE myTable
    # SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# On all newly created Delta Tables, you can use Spark configuration settings to set this property to true in a notebook or on a cluster
    # spark.databricks.delta.properties.defaults.enableChangeDataFeed

# COMMAND ----------

# CDF data follows the same retention policy of the table
# If you run a VACUUM command on the table, CDF data is also deleted

# COMMAND ----------

# When to use CDF

# We use CDF for sending incremental data changes to downstream tables in a multi-hop architecture
    # These changes should include updates and deletes

# If these changes are append-only, then there is no need to use CDF. We can directly stream these changes from the table

# Use CDF when only a small fraction of records updated in each batch. Such updates usually received from external sources in CDC format

# Don't use CDF if most of the records in the table are updated or if the table is completely overwritten in each batch

# COMMAND ----------



# COMMAND ----------

# Let us enable CDF on our existing customer table.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE
# MAGIC   customers_silver
# MAGIC SET
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED customers_silver;
# MAGIC
# MAGIC -- Table Properties     [delta.enableChangeDataFeed=true,delta.minReaderVersion=1,delta.minWriterVersion=4]

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Editing table properties will add a new version to the table
# MAGIC
# MAGIC DESCRIBE HISTORY customers_silver;

# COMMAND ----------

# Let us land more data files and process them till our "customer_silver" table
bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_orders_silver()
bookstore.porcess_customers_silver()

# The above functions are present in "copy_datasets" notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let us read the changes recorded by CDF after these new data merged in our customer table
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   table_changes("customers_silver", 2);

# COMMAND ----------

# Let us land another file in our source directory and process it
bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_orders_silver()
bookstore.porcess_customers_silver()

# COMMAND ----------

# Let us read the new table changes
# In python, we can read the recorded CDF data by adding 2 options to a "readStream" query which are "readChangeData" with a value "True" and the "startingVersion"

cdf_df = (
    spark.readStream.format("delta")
    .option("readChangeData", True)
    .option("startingVersion", 2)
    .table("customers_silver")
)

cdf_df.display()

# COMMAND ----------

# Let us see what happened in the table directory in the underlying storage
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/customers_silver")
display(files)

# There is an additional metadata directory nested in our table directory called "_change_data"

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/customers_silver/_change_data/")
display(files)

# This directory contains Parquet fiiles where the CDF data is recorded

# COMMAND ----------


