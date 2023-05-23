# Databricks notebook source
# Trigger Intervals
# -----------------

# When defining a Streaming write, the "trigger()" method specify when the system should process the next set of data. And this is called Trigger interval.
# By default, if you do not provide any Trigger Interval, the data would be processed every half second (500 ms).

# Fixed Interval                    -   Process data in micro-batches at the user-specified intervals
#       .trigger(processingTime="5 minutes")

# Triggered Batch                   -   Process all available data in a single batch, then stop
#       .trigger(once=True)

# Triggered Micro-batches           -   Process all available data in multiple micro-batches, then stop
#       .trigger(availableNow=True)

# COMMAND ----------

# Output Modes
# ------------

# Append  (default)                              -   Only newly appended rows are incrementally appended to the target table with each batch
#           .outputMode("append")

# Complete                                       -   The target table is overwritten with each batch
#           .outputMode("complete")

# COMMAND ----------

# Checkpointing
# -------------

# Databricks create checkpoints by storing the current state of your streaming job to cloud storage
# Checkpointing allow the streaming engine to track the process of your stream processing
# Checkpoints cannot be shared between several streams. A seperate checkpoint location is required for every streaming write to ensure processing guarantees

#           .option("checkpointLocation", "/path")

# COMMAND ----------

# Structured Streaming provide 2 guarantees:

#   Fault Tolerance
        # Checkpointing + Write ahead logs
        # They allow to record the offset range of data being processed during each trigger interval to track your stream progress


#   Exactly-once guarantee
        # Streaming sinks are designed to be idempotent. That is, multiple writes of the same data, identified by the offset, do not result in duplicates being written to the sink.

# COMMAND ----------

# Some operations are not supported by streaming DataFrame
#   Sorting
#   Deduplication

# There are advanced streaming methods like Windowing and Watermarking that can help to do such operations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   books;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books;
# MAGIC
# MAGIC -- Type       MANAGED
# MAGIC -- Location   dbfs:/user/hive/warehouse/books
# MAGIC -- Provider   delta

# COMMAND ----------

spark.readStream.table("books").createOrReplaceTempView("books_streaming_tmp_vw")

# "spark.readStream" method allow to query a Delta Table as a Stream source
# And from there we can register a temporary view against the stream source

# The temporary view created here is a streaming temporary view that allow to apply most transformation in SQL the same way as we would with static data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   books_streaming_tmp_vw;
# MAGIC
# MAGIC -- Here we see a Streaming result as the query is still running waiting for any new data to be displayed here 
# MAGIC -- Generally we do not display a Streaming result unless a human is actively monitoring the out of a query during development or live dashboarding

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   author,
# MAGIC   count(book_id) AS total_books
# MAGIC FROM
# MAGIC   books_streaming_tmp_vw
# MAGIC GROUP BY
# MAGIC   author;
# MAGIC
# MAGIC -- Because we are querying a Streaming Temporary View, this become a streaming query that execute infinitely

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   books_streaming_tmp_vw
# MAGIC ORDER BY
# MAGIC   author;
# MAGIC
# MAGIC -- AnalysisException: Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode; line 5 pos 0;%sql
# MAGIC
# MAGIC -- While working with Streaming data, some operations are not supported like sorting

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMP VIEW author_count_tmp_vw AS (
# MAGIC   SELECT
# MAGIC     author,
# MAGIC     count(book_id) AS total_books
# MAGIC   FROM
# MAGIC     books_streaming_tmp_vw
# MAGIC   GROUP BY
# MAGIC     author
# MAGIC );
# MAGIC
# MAGIC -- In order to persist incremental results we need first to pass our logic back to PySpark DataFrame API
# MAGIC -- Here we are creating another temporary view
# MAGIC -- Since we are creating this temporary view from the result of a query against a streaming temporary view, this new temporary view is also a streaming temporary view

# COMMAND ----------

spark.table("author_count_tmp_vw").writeStream.trigger(
    processingTime="4 seconds"
).outputMode("complete").option(
    "checkpointLocation", "dbfs:/FileStore/demo/author_counts_checkpoint"
).table(
    "author_counts"
)

# In PySpark DataFrame API, we can use "spark.table()" to load data from a Streaming temporary view back to a DataFrame
# Spark always loads streaming views as a streaming DataFrame and static views as a static DataFrame. Incremental logic must be defined from the very beginning with read logic to support later an incremental writing.
# For aggreagtion streaming queries, we must always use "complete" outputMode to overwrite the table with the new calculation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   author_counts;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   books
# MAGIC VALUES
# MAGIC   (
# MAGIC     "B19",
# MAGIC     "Introduction to Modeling and Simulation",
# MAGIC     "Mark W. Spong",
# MAGIC     "Computer Science",
# MAGIC     25
# MAGIC   ),
# MAGIC   (
# MAGIC     "B20",
# MAGIC     "Robot Modeling and Control",
# MAGIC     "Mark W. Spong",
# MAGIC     "Computer Science",
# MAGIC     30
# MAGIC   ),
# MAGIC   (
# MAGIC     "B21",
# MAGIC     "Turing's Vision: The Birth of Computer Science",
# MAGIC     "Chris Bernhardt",
# MAGIC     "Computer Science",
# MAGIC     35
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   books
# MAGIC VALUES
# MAGIC   (
# MAGIC     "B16",
# MAGIC     "Hands-On Deep Learning Algorithms with Python",
# MAGIC     "Sudharsan Ravichandiran",
# MAGIC     "Computer Science",
# MAGIC     25
# MAGIC   ),
# MAGIC   (
# MAGIC     "B17",
# MAGIC     "Neural Network Methods in Natural Language Processing",
# MAGIC     "Yoav Goldberg",
# MAGIC     "Computer Science",
# MAGIC     30
# MAGIC   ),
# MAGIC   (
# MAGIC     "B18",
# MAGIC     "Understanding digital signal processing",
# MAGIC     "Richard G. Lyons",
# MAGIC     "Computer Science",
# MAGIC     35
# MAGIC   )

# COMMAND ----------

spark.table("author_count_tmp_vw").writeStream.trigger(availableNow=True).outputMode(
    "complete"
).option("checkpointLocation", "dbfs:/FileStore/demo/author_counts_checkpoint").table(
    "author_counts"
).awaitTermination()

# In this case we can use the "awaitTermination()" method to block the execution of any cell in this notebook until the incremental batch write has succeeded

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   author_counts;

# COMMAND ----------


