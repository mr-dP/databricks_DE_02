-- Databricks notebook source
SELECT
  *
FROM
  customers;

-- COMMAND ----------

DESCRIBE EXTENDED customers;

-- COMMAND ----------

-- SparkSQL has built-in functionality to directly interact with JSON data stored as Strings
-- We can simply use the colon syntax to traverse nested Data Structures
SELECT
  customer_id,
  profile :first_name,
  profile :address :country
FROM
  customers;

-- COMMAND ----------

SELECT
  profile
FROM
  customers
LIMIT
  1;

-- COMMAND ----------

SELECT
  schema_of_json(
    ' { "first_name" :"Thomas",
    "last_name" :"Lane",
    "gender" :"Male",
    "address": { "street" :"06 Boulevard Victor Hugo",
    "city" :"Paris",
    "country" :"France" } }'
  );

-- COMMAND ----------

-- SparkSQL also has the ability to parse JSON object into STRUCT types
-- STRUCT is a native Spark type with nested attributes
SELECT
  from_json(
    profile,
    schema_of_json(
      ' { "first_name" :"Thomas",
      "last_name" :"Lane",
      "gender" :"Male",
      "address": { "street" :"06 Boulevard Victor Hugo",
      "city" :"Paris",
      "country" :"France" } }'
    )
  ) AS profile_struct
FROM
  customers;
-- "from_json()" function require the schema of the JSON object

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW parsed_customers AS
SELECT
  customer_id,
  from_json(
    profile,
    schema_of_json(
      '{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}'
    )
  ) AS profile_struct
FROM
  customers;
  
SELECT
  *
FROM
  parsed_customers;

-- COMMAND ----------

DESCRIBE EXTENDED parsed_customers;
-- The "profile_struct" column has a "STRUCT" Data type

-- COMMAND ----------

-- With STRUCT type we can interact with the sub-fields with standard dot syntax
SELECT
  customer_id,
  profile_struct.first_name,
  profile_struct.address.country
FROM
  parsed_customers;

-- COMMAND ----------

-- Once the JSON String is converted to a STRUCT type, we can use "*" operation to flatten fields into columns
CREATE
OR REPLACE TEMP VIEW customers_final AS
SELECT
  customer_id,
  profile_struct.*,
  profile_struct.address.*
FROM
  parsed_customers;
  
SELECT
  *
FROM
  customers_final;

-- COMMAND ----------

SELECT
  order_id,
  customer_id,
  books
FROM
  orders;

-- Here the "books" column is an Array of Struct type

-- COMMAND ----------

-- "explode()" function allow us to put each element of an Array on its own row
SELECT
  order_id,
  customer_id,
  explode(books) AS book
FROM
  orders;

-- COMMAND ----------

-- "collect_set()" aggregation function allow us to collect unique values for a field including fields within Arrays
SELECT
  customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM
  orders
GROUP BY
  customer_id;

-- COMMAND ----------

SELECT
  customer_id,
  collect_set(books.book_id) AS before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM
  orders
GROUP BY
  customer_id;

-- COMMAND ----------

CREATE
OR REPLACE VIEW orders_enriched AS
SELECT
  *
FROM
  (
    SELECT
      *,
      explode(books) AS book
    FROM
      orders
  ) o
  INNER JOIN books b ON o.book.book_id = b.book_id;
  
SELECT
  *
FROM
  orders_enriched;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW orders_updates AS
SELECT
  *
FROM
  parquet.`dbfs:/FileStore/demo-datasets/bookstore/orders-new`;
  
SELECT
  *
FROM
  orders
UNION
SELECT
  *
FROM
  orders_updates;

-- COMMAND ----------

SELECT
  *
FROM
  orders
INTERSECT
SELECT
  *
FROM
  orders_updates;

-- The "INTERSECT" command returns all rows ound in both relations

-- COMMAND ----------

SELECT
  *
FROM
  orders
MINUS
SELECT
  *
FROM
  orders_updates;

-- COMMAND ----------

-- "PIVOT" clause is used to change data perspective. We can get the aggregated values based on a specific column values which will be turned to multiple columns used in "SELECT" clause
CREATE
OR REPLACE TABLE transactions AS
SELECT
  *
FROM
  (
    SELECT
      customer_id,
      book.book_id AS book_id,
      book.quantity AS quantity
    FROM
      orders_enriched
  ) PIVOT (
    sum(quantity) FOR book_id IN (
      'B01',
      'B02',
      'B03',
      'B04',
      'B05',
      'B06',
      'B07',
      'B08',
      'B09',
      'B010',
      'B011',
      'B012'
    )
  );
  
SELECT
  *
FROM
  transactions;

-- COMMAND ----------


