-- Databricks notebook source
-- SELECT * FROM file_format.`/path/to/file`;

-- This work well with self-describing formats that have well defined schema like JSON and PARQUET
-- IT is not very useful with non-self-describing formats like CSV or TSV

-- COMMAND ----------

-- When working with Text based files which include JSON, CSV, TSV and TXT format, you can use "text" format to extract data as raw Strings
--    SELECT * FROM text.`/path/to/file`
-- This can be useful when input data could be corrupted

-- COMMAND ----------

-- In some cases we need the binary represetation of file's contents, for example, when dealing with images and unstrutured data.
--    SELECT * FROM binaryFile.`/path/to/file`

-- COMMAND ----------

-- To load data from files into Delta Tables, we use CTAS statements

--    CREATE TABLE table_name
--    AS SELECT * FROM file_format.`/path/to/file`

-- CTAS statements are useful for external data ingestion from sources with well defined schema

-- CTAS statements also do not support specifying additional file options.

-- COMMAND ----------

-- For such a format that requires additional options, we need another solution that supports options

-- CREATE TABLE  table_name
-- (col_name1 col_type1, ...)
-- USING data_source
-- OPTIONS (key1 = val1, key2 = vaL2, ...)
-- LOCATION = path

-- By adding the "USING" keyword, we specify the external data source type for example CSV format and with any additional OPTIONS and you need to specify a LOCATION to where these files are stored

-- With this command we are always creating an External Table. The table here is just a reference to the files.
-- Here is no data moving during table creation
-- These files are kept in its original format. We are creating here a non-Delta table

-- COMMAND ----------

-- We can create a Temporary View referring to the External data source, and then query this temporary view to create a table using CTAS statement
-- In this way we are extracting the data from the External data source and load it in a Delta Table

-- CREATE TEMPORARY VIEW temp_view_name (col_name1 col_type1, ...)
-- USING data_source
-- OPTIONS (key1 = val1, key2 = val2, ...)
-- LOCATION = path;

-- CREATE TABLE table_name
-- AS SELECT * FROM temp_view_name;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %run /Repos/databricks_DE_02/databricks_DE_02/helpers/copy_datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/demo-datasets/bookstore/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT
  *
FROM
  json.`dbfs:/FileStore/demo-datasets/bookstore/customers-json/export_001.json`;

-- COMMAND ----------

-- We can use wild-card character to query multiple files simultaneously
SELECT
  *
FROM
  json.`dbfs:/FileStore/demo-datasets/bookstore/customers-json/export_*.json`;

-- COMMAND ----------

-- We can query a complete directory of files assuming all the files in the directory have the same format and schema
SELECT
  *
FROM
  json.`dbfs:/FileStore/demo-datasets/bookstore/customers-json/`;

-- COMMAND ----------

SELECT
  count(*)
FROM
  json.`dbfs:/FileStore/demo-datasets/bookstore/customers-json/`;

-- COMMAND ----------

-- While reading multiple files, it is useful to add the input_file_name() function
SELECT
  *,
  input_file_name() source_file
FROM
  json.`dbfs:/FileStore/demo-datasets/bookstore/customers-json/`;

-- COMMAND ----------

SELECT
  *
FROM
  text.`dbfs:/FileStore/demo-datasets/bookstore/customers-json/`;
  
-- This store each line of the file as a row with one string column named "value"

-- COMMAND ----------

SELECT
  *
FROM
  binaryFile.`dbfs:/FileStore/demo-datasets/bookstore/customers-json/`;

-- We can use "binaryFile" to extract the raw bytes and some metadata of files

-- COMMAND ----------

SELECT
  *
FROM
  csv.`dbfs:/FileStore/demo-datasets/bookstore/books-csv`

-- COMMAND ----------

CREATE TABLE books_csv (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
) USING CSV
OPTIONS (header = "true", delimiter = ";")
LOCATION "dbfs:/FileStore/demo-datasets/bookstore/books-csv";

-- COMMAND ----------

SELECT
  *
FROM
  books_csv;

-- COMMAND ----------

-- When working with CSV files as data source, it is important to ensure that column orders does not change if additional data files will be added to the source directory.
-- Spark will always load data and apply column names and data types in the order specified dueing table creation

-- COMMAND ----------

DESCRIBE EXTENDED books_csv;
-- Type                   EXTERNAL
-- Provider               CSV
-- Location               dbfs:/FileStore/demo-datasets/bookstore/books-csv
-- Storage Properties     [delimiter=;, header=true]

-- COMMAND ----------

-- MAGIC   %python
-- MAGIC
-- MAGIC   files = dbutils.fs.ls("dbfs:/FileStore/demo-datasets/bookstore/books-csv")
-- MAGIC   display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.read.table("books_csv").write.mode("append").format("csv").option(
-- MAGIC     "header", "true"
-- MAGIC ).option("delimiter", ";").save("dbfs:/FileStore/demo-datasets/bookstore/books-csv")

-- COMMAND ----------

-- MAGIC   %python
-- MAGIC
-- MAGIC   files = dbutils.fs.ls("dbfs:/FileStore/demo-datasets/bookstore/books-csv")
-- MAGIC   display(files)

-- COMMAND ----------

SELECT
  COUNT(*)
FROM
  books_csv;

-- Even with the new data have been successfully written to the table directory we are still unable to see the new data because Spark automatically caceh the underlying data in the local storage to ensure that on subsequent queries Spark will provide the optimal performance by just querying the local cache

-- The External CSV file is not configured to tell Spark that it should refresh this data

-- COMMAND ----------

-- We can manually refresh the cache of our data by running the "REFRESH TABLE" command
REFRESH TABLE books_csv;

-- Refreshing a table will invalidate the cache, meaning we will need to rescan our original data source and pull all data back into memory. For very large dataset this may take a significant amount of time.

-- COMMAND ----------

SELECT
  COUNT(*)
FROM
  books_csv;

-- COMMAND ----------

CREATE TABLE customers AS
SELECT
  *
FROM
  json.`dbfs:/FileStore/demo-datasets/bookstore/customers-json`;

DESCRIBE EXTENDED customers;

-- Type          MANAGED
-- Location      dbfs:/user/hive/warehouse/customers
-- Provider      delta

-- CTAS statements automatically infers schema information from query result. And do not support manual schema declaration

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT
  *
FROM
  csv.`dbfs:/FileStore/demo-datasets/bookstore/books-csv`;
  
SELECT
  *
FROM
  books_unparsed;

-- COMMAND ----------

-- This temporary view allow us to specify file options
CREATE TEMPORARY VIEW books_tmp_vw (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
)
USING CSV
OPTIONS (
  path = "dbfs:/FileStore/demo-datasets/bookstore/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

-- We will use the temporary view as the source for our CTAS statement to successfully register the Delta Table
CREATE TABLE books AS
SELECT
  *
FROM
  books_tmp_vw;

SELECT
  *
FROM
  books;

-- COMMAND ----------

DESC EXTENDED books;

-- Type        MANAGED
-- Location    dbfs:/user/hive/warehouse/books
-- Provider    delta

-- COMMAND ----------


