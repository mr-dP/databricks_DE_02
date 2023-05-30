# Databricks notebook source
# OPTIMIZE command
# ----------------

# Delta Lake supports compacting small files by manually running the "OPTIMIZE" command.
# In this case, the target file size is typically 1 GB

# COMMAND ----------

# Auto Optimize
# -------------

# Delta Lake can also automatically compact small files
# This can be achieved during individual writes to a Delta Table

# Auto Optimize consists of 2 complementary features:
    # Optimize Writes:
        # With Optimize Writes enabled, Dtabricks attempts to write out 128 MB files for each partition

    # Auto Compaction
        # Auto Compaction will check after individual write if files can further be compacted
        # If yes, it runs an optimized job, but this time with 128 MB file size instead of the 1 GB file size used in the standard OPTIMIZE

# Auto Compaction does not support Z-Ordering as Z-Orderingis significantly more expensive than just compaction

# COMMAND ----------

# You can explicitly enable optimized writes and auto compaction when creating or altering a Delta Table

# New tables
            # CREATE TABLE myTable (id INT, name STRING)
            # TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,
            #             delta.autoOptimize.autoCompact = true)

# Existing table
            # ALTER TABLE myTable
            # SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,
            #                 delta.autoOptimize.autoCompact = true)

# In addition, you can enable or disable both of these features for Spark Session with the following 2 configurations:
            # spark.databricks.delta.optimizeWrite.enabled
            # spark.databricks.delta.autoCompact.enabled
    # These session configurations take precedence over the table properties allowing you to better control when to opt in or to opt out of these featuresHavin

# COMMAND ----------

# Having many small files is not always a problem since it can lead to better data skipping
    # Also, it can help minimize rewrites during some operations like merges and deletes

# For such operations, Databricks can automatically tune the file size of Delta tables.
    # For example, when executing a frequent merge operations on the table, then Optimized Writes and Auto Compaction will generate data files smaller than the default 128 MB. This auto tuning helps in reducing the duration of future merge operations.

# COMMAND ----------


