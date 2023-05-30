# Databricks notebook source
# In Spark Strutured Streaming, incremental tables or streaming tables are append-only data sources. So, data can only be appended in these tables

# While static tables typically contain data that may be updated, deleted or overwritten
    # Such data sources are not streamable becuase they break the above append-only requirements of streaming sources.

# COMMAND ----------

# When performaing stream-static join, the resulting table is an incremental table
# this pattern will take advantage of the Delta Lake's guarantee that the latest version of the static Delta Table is returned each time it is queried

# In stream-static join, the streaming portion of the join drive the join process.
    # Only new data appearing on the streaming side of the join will trigger the processing
    # while adding new records into the static table will not automatically trigger updates to the result of the stream-static join

# In stream-static join, only matched records at the time the stream is processed will be presented in the resulting table
    # Unmatched records at the time the stream is processed will be missed from the resulting table

# can we buffer these unmatched records as a streaming state to be matched later?
    # No. Streaming static joins are not stateful
    # We cannot configure our query to wait for new records to appear in the static side prior to calculating the results
    # When leveraging stream-static joins, make sure to be aware of these limitations for unmatched records.
    # for this, you can configure a separate batch job to find and insert these missed records.

# COMMAND ----------



# COMMAND ----------

# We will create a silver table "books_sales" by joining the "orders" streaming table with the "current_books" static table
    # The "current_books" table is no longer streamable. This table is updated using batch overwrite logic so it breaks the requirement of an even appending source for structured streaming
    # Delta Lake guarantess that the latest version of a static table is returned each time it is queried in a streaming static join

# COMMAND ----------



# COMMAND ----------

# Lets write our streaming query

from pyspark.sql.functions import col, explode


def process_books_sales():

    # We read our "orders" table as a streaming source using "spark.readStream"
    orders_df = spark.readStream.table("orders_silver").withColumn(
        "book", explode("books")
    )

    # We read the "current_books" static table using "spark.read"
    books_df = spark.read.table("current_books")

    # We can join the 2 DataFrames as usual
    query = (
        orders_df.join(books_df, orders_df.book.book_id == books_df.book_id, "inner")
        .writeStream.outputMode("append")
        .option(
            "checkpointLocation", "dbfs:/FileStore/demo_pro/checkpoints/books_sales"
        )
        .trigger(availableNow=True)
        .table("books_sales")
    )

    query.awaitTermination()


process_books_sales()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   books_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   books_sales;

# COMMAND ----------

# When performing Stream-Static join, the streaming portion of the join drives the joining process
# Only new data appearing on the streaming side of the query will trigger the processing
# And we are guaranteed to get the latest version of the static table during each micro-batch transaction

# COMMAND ----------

# Here we will land a new data file in our dataset source directory and propagate the data only to the static table
bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_books_silver()
bookstore.process_current_books()

process_books_sales()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If we check the number of records again, we see that we have the same number of records
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   books_sales;
# MAGIC
# MAGIC -- This confirm that our streaming static join did not trigger by only appending data to the static table

# COMMAND ----------

# Let us propagate our new data to the "orders" streaming table and re-execute our stream-static join
bookstore.porcess_orders_silver()

process_books_sales()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If we check the number of records one more time, we see that our stream-static join has been triggered this time by the new data appended to the streaming table
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   books_sales;

# COMMAND ----------


