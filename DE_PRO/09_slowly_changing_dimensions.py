# Databricks notebook source
# Slowly Changing Dimensions (SCD) is a data management concept that determines how tables handle data which change over time
# For example, whether you want to overwrite values in the table or maybe retain their history. this can be determined by the SCD type to implement

# COMMAND ----------

# There are 3 types of SCD:

# Type 0: No changes allowed
    # Tables of this type are either static or append-only tables
    # For example, static lookup tables



# Type 1: Overwrite
    # The new data overwrite the existing one. Thus, the existing data is lost as it is not stored anywhere else
    # This type is useful only when you care about the current values rather than historic comparisons
    # No history retained. When new data arrives, the old attribute's values in the table rows are overwritten with the new values

# Type 2: Add new row for each change and mark the old as obsolete
    # A new record is added with the changed data values. This new record becomes the current active record, while the old record is marked as no longer active.
    # Type 2 SCD retains the full history of values
    # For example, "products" table that track prices changes over time
    # In order to support SCD Type 2 changes, we need to add 3 columns to our table:
        #   a current column: which is a flag that indicates whether the record is the current version or not
        #   effective or start date: the date from which the record version on active
        #   end date: the date to which the record version was active
    # When new data arrives, a new record is added the the table with the updated attributes values

# COMMAND ----------

# Why not using Delta Time Travel feature to access the historical versions of the data that changes?

# Delta Time Travel does not scale well in cost and latency to provide a long term versioning solution.
# Running a VACUUM command will cause the table historical versions to be deleted.

# COMMAND ----------



# COMMAND ----------

# We will create the "books" silver table.
# For this we will use a Type 2 SCD table to record the books data.
# The idea is to keep trace of all the price modifications of a book, so we can verify our order's total amount at any given time.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let us first prepare our MERGE statement that will be used in a "foreachBatch()" function call
# MAGIC MERGE INTO books_silver
# MAGIC USING (
# MAGIC 	SELECT updates.book_id AS merge_key,
# MAGIC 		updates.*
# MAGIC 	FROM updates
# MAGIC 	
# MAGIC 	UNION ALL
# MAGIC 	
# MAGIC 	SELECT NULL AS merge_key,
# MAGIC 		updates.*
# MAGIC 	FROM updates
# MAGIC 	JOIN books_silver ON updates.book_id = books_silver.book_id
# MAGIC 	WHERE books_silver.CURRENT = true
# MAGIC 		AND updates.price <> books_silver.price
# MAGIC 	) staged_updates
# MAGIC 	ON books_silver.book_id = merge_key
# MAGIC WHEN MATCHED
# MAGIC 	AND books_silver.CURRENT = true
# MAGIC 	AND books_silver.price <> staged_updates.price
# MAGIC 	THEN
# MAGIC 		UPDATE
# MAGIC 		SET CURRENT = false,
# MAGIC 			end_date = staged_updates.updated
# MAGIC WHEN NOT MATCHED
# MAGIC 	THEN
# MAGIC 		INSERT (
# MAGIC 			book_id,
# MAGIC 			title,
# MAGIC 			author,
# MAGIC 			price,
# MAGIC 			current,
# MAGIC 			effective_date,
# MAGIC 			end_date
# MAGIC 			)
# MAGIC 		VALUES (
# MAGIC 			staged_updates.book_id,
# MAGIC 			staged_updates.title,
# MAGIC 			staged_updates.author,
# MAGIC 			staged_updates.price,
# MAGIC 			true,
# MAGIC 			staged_updates.effective_date,
# MAGIC 			NULL
# MAGIC 			);
# MAGIC
# MAGIC -- Here we are merging our micro-batch updates into the "books_silver" table based on a custom "merge_key"
# MAGIC -- In SCD Type 2, old records need to be marked as no longer valid. For this, we use the "book_id" as a "merge_key"
# MAGIC -- So when there is an update matched with a currently active record in the table, we update this records's "current" status to false and we set its "end_date"
# MAGIC -- In the same time, the received updates need to be inserted as separate records. This is why we are reprocessing them with a "null" merge_key. This allows to insert these uodates using the "NOT MATCHED" clause.
# MAGIC -- And, we are inserting the new records with a "current" status equal to "true" and without "end_date"

# COMMAND ----------

# Let us now integrate the above MERGE statement into a function to be called with "foreachBatch()" function


def type2_upsert(mircoBatchDF, batch):
    mircoBatchDF.createOrReplaceTempView("updates")

    sql_query = """
        MERGE INTO books_silver
        USING (
            SELECT updates.book_id AS merge_key,
                updates.*
            FROM updates
            
            UNION ALL
            
            SELECT NULL AS merge_key,
                updates.*
            FROM updates
            JOIN books_silver ON updates.book_id = books_silver.book_id
            WHERE books_silver.CURRENT = true
                AND updates.price <> books_silver.price
            ) staged_updates
            ON books_silver.book_id = merge_key
        WHEN MATCHED
            AND books_silver.CURRENT = true
            AND books_silver.price <> staged_updates.price
            THEN
                UPDATE
                SET CURRENT = false,
                    end_date = staged_updates.updated
        WHEN NOT MATCHED
            THEN
                INSERT (
                    book_id,
                    title,
                    author,
                    price,
                    current,
                    effective_date,
                    end_date
                    )
                VALUES (
                    staged_updates.book_id,
                    staged_updates.title,
                    staged_updates.author,
                    staged_updates.price,
                    true,
                    staged_updates.updated,
                    NULL
                    )
    """

    mircoBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- because we are using SQL to write our DELTA Table, we need to make sure that the table exists before we begin
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS books_silver (
# MAGIC   book_id STRING,
# MAGIC   title STRING,
# MAGIC   author STRING,
# MAGIC   price DOUBLE,
# MAGIC   current BOOLEAN,
# MAGIC   effective_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP
# MAGIC );

# COMMAND ----------

# Now we can execute a Streaming query to process the "books" data from the "bronze" table

from pyspark.sql.functions import from_json, col


def process_books():
    schema = (
        "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
    )

    query = (
        spark.readStream.table("bronze")
        .filter("topic = 'books'")
        .select(from_json(col("value").cast("string"), schema).alias("v"))
        .select("v.*")
        .writeStream.foreachBatch(type2_upsert)
        .option(
            "checkpointLocation", "dbfs:/FileStore/demo_pro/checkpoints/books_silver"
        )
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

# COMMAND ----------

process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
books_df.display()

# COMMAND ----------

# We are going to create another silver table called "current_books" to represent only the latest books information

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We will create this table with batch overwrite logic using "CREATE OR REPLACE TABLE" syntax
# MAGIC -- Each time we run this query, the contents of the table will be overwritten
# MAGIC
# MAGIC CREATE
# MAGIC OR REPLACE TABLE current_books AS
# MAGIC SELECT
# MAGIC   book_id,
# MAGIC   title,
# MAGIC   author,
# MAGIC   price
# MAGIC FROM
# MAGIC   books_silver
# MAGIC WHERE
# MAGIC   current IS TRUE;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   current_books
# MAGIC ORDER BY
# MAGIC   book_id;

# COMMAND ----------


