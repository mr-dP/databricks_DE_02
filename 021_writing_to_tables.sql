-- Databricks notebook source
CREATE TABLE orders AS
SELECT
  *
FROM
  parquet.`dbfs:/FileStore/demo-datasets/bookstore/orders`;

-- COMMAND ----------

SELECT
  *
FROM
  orders;

-- COMMAND ----------

-- There are multiple benefits to overwriting tables instead of deleting and recreating tables.
-- The old version of the table will still exist and can easily retrive old data using Time Travel
-- Overwriting a table is much faster because it does not need to list the directory recursively or delete any files.
-- It is an atomic operation. Concurrent queries can still read the table while you are overwriting it.
-- Due to the ACID transaction guarantees if overwriting the table fails the table will be in its previous state.

-- COMMAND ----------

-- The method to accomplish complete overwrite is to use "CREATE OR REPLACE TABLE".
-- "CREATE OR REPLACE TABLE" statement fully replace the content of the table each time they execute.
CREATE
OR REPLACE TABLE orders AS
SELECT
  *
FROM
  parquet.`dbfs:/FileStore/demo-datasets/bookstore/orders`;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

-- The second method to overwrite table data is to use "INSERT OVERWRITE" statement.
-- It provides uniquely identical output as above. That is, data in the target table will be replaced by data from the query
-- It can only overwrite an existing table and not creating a new one like "CREATE OR REPLACE" statement
-- It can overwrite only the new records that match the current table schema. It is a safer technique for overwriting an existing table without the risk of modifying the table schema
INSERT
  OVERWRITE orders
SELECT
  *
FROM
  parquet.`dbfs:/FileStore/demo-datasets/bookstore/orders`;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

-- If you try to INSERT OVERWRITE the data with different schema, it will generate an Exception
INSERT
  OVERWRITE orders
SELECT
  *,
  current_timestamp()
FROM
  parquet.`dbfs:/FileStore/demo-datasets/bookstore/orders`;

-- Error in SQL statement: AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: 2a411946-c747-4abc-a4fd-f7304accb8dd).

-- COMMAND ----------

-- append records to tables - the easiest method is to use "INSERT INTO" statements
INSERT INTO
  orders
SELECT
  *
FROM
  parquet.`dbfs:/FileStore/demo-datasets/bookstore/orders-new`;

-- "INSERT INTO" does not have any built-in guarantees to prevent inserting the same records multiple times.
--    Re-executing the query will write the same records to the target table resulting in duplicate records.

-- COMMAND ----------

SELECT
  COUNT(*)
FROM
  orders;

-- COMMAND ----------

SELECT
  *
FROM
  customers;

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW customers_updates AS
SELECT
  *
FROM
  json.`dbfs:/FileStore/demo-datasets/bookstore/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
  WHEN MATCHED
  AND c.email IS NULL
  and u.email IS NOT NULL THEN
UPDATE
SET
  email = u.email,
  updated = u.updated
  WHEN NOT MATCHED THEN
INSERT
  *;

-- With "MERGE INTO" statement you can upsert data from a source table/view/dataframe into the target Delta Table

-- In a Merge operation, updates, inserts and deletes are completed in a single atomic transaction
-- Merge operation is a great solution for avoiding any duplicates when inserting records

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW books_updates (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
) USING CSV OPTIONS(
  path = "dbfs:/FileStore/demo-datasets/bookstore/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT
  *
FROM
  books_updates;

-- COMMAND ----------

SELECT
  *
FROM
  books;

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id
AND b.title = u.title
WHEN NOT MATCHED
AND u.category = "Computer Science" THEN
INSERT
  *;

-- One of the main benefit of the Merge operation is to avoid duplicates. So, if we re-run this statement, it will not re-insert those records as they are already in the table

-- COMMAND ----------

SELECT
  *
FROM
  books;

-- COMMAND ----------


