-- Databricks notebook source
SELECT
  *
FROM
  orders;

-- COMMAND ----------

-- Higher Order Functions allow you to work directly with hierarchical data like Arrays and Map type objects

-- COMMAND ----------

SELECT
  order_id,
  books,
  filter(books, i -> i.quantity >= 2) AS multiple_copies
FROM
  orders;

-- "filter()" function filter an Array using a given Lambda functions
-- In this example, we are creating a new column called "multiple_copies" where we filter the "books" column to extract only those books that have "quantity" greater or equal to 2

-- COMMAND ----------

SELECT
  order_id,
  multiple_copies
FROM
  (
    SELECT
      order_id,
      books,
      filter(books, i -> i.quantity >= 2) AS multiple_copies
    FROM
      orders
  )
WHERE
  size(multiple_copies) > 0;

-- COMMAND ----------

SELECT
  order_id,
  books,
  transform(books, b -> CAST(b.subtotal * 0.8 AS INT)) AS subtotal_after_discount
FROM
  orders;

-- "transform()" function is used to apply a transformation on all the items in an Array and extract the transformed value

-- COMMAND ----------



-- COMMAND ----------

-- User Defined Functions (UDFs) allow us to register a custom combination of SQL logic as function in a database making these methods re-usable in any SQL query.
-- UDF functions leverage SparkSQL directly maintaing all the optimization of Spark when applying  your custom logic to large datasets.

-- COMMAND ----------

-- Example of creating an UDF
CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@") [1])

-- At minimum, it requires a function name, optional parameters, the type to be returned and some custom logic

-- COMMAND ----------

SELECT
  email,
  get_url(email) domain
FROM
  customers;

-- COMMAND ----------

DESCRIBE FUNCTION get_url

-- UDFs are permanant objects that are persisted to the database so that you can use them between different Spark Sessions and Notebooks

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url

-- Body:          concat("https://www.", split(email, "@") [1])

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profit organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat(
            "Unknown extension for domain: ",
            split(email, "@") [1]
          )
      END;

-- COMMAND ----------

SELECT
  email,
  site_type(email) AS domain_category
FROM
  customers;

-- COMMAND ----------

-- UDF function is really powerfull
-- Everything is evaluated natively in Spark and so it is optimized for parallel execution

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;

-- COMMAND ----------


