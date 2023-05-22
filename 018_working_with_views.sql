-- Databricks notebook source
-- Views
-- -----

-- Logical query against source tables

-- Types
-- -----
--    (Stored) Views          (Persisted in DB)     (Dropped only by DROP VIEW)
--    Temp Views              (Session-scoped)      (Dropped when session ends)
--    Global Temp views       (Clster-scoped)       (Dropped when cluster restarted)      (SELECT * FROM global_temp.view_name)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO
  smartphones
VALUES
  (1, 'iPhone 14', 'Apple', 2022),
  (2, 'iPhone 13', 'Apple', 2021),
  (3, 'iPhone 6', 'Apple', 2014),
  (4, 'iPad Air', 'Apple', 2013),
  (5, 'Galaxy S22', 'Samsung', 2022),
  (6, 'Galaxy Z Fold', 'Samsung', 2022),
  (7, 'Galaxy S9', 'Samsung', 2016),
  (8, '12 Pro', 'Xiaomi', 2022),
  (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
  (10, 'Redmi Note 11', 'Xiaomi', 2022);

-- COMMAND ----------

show tables;

-- COMMAND ----------

CREATE VIEW view_apple_phones AS
SELECT
  *
FROM
  smartphones
WHERE
  brand = 'Apple';

-- COMMAND ----------

SELECT
  *
FROM
  view_apple_phones;

-- COMMAND ----------

show tables;

-- COMMAND ----------

CREATE TEMPORARY VIEW temp_view_phones_brands AS
SELECT
  DISTINCT brand
FROM
  smartphones;

SELECT
  *
FROM
  temp_view_phones_brands;

-- Since it is temporary, it is not persisted to any database

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_temp_view_latest_phones AS
SELECT
  *
FROM
  smartphones
WHERE
  year > 2020
ORDER BY
  year DESC;

-- COMMAND ----------

SELECT
  *
FROM
  global_temp_view_latest_phones;
  
-- Error in SQL statement: AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `global_temp_view_latest_phones` cannot be found. Verify the spelling and correctness of the schema and catalog.

-- COMMAND ----------

SELECT
  *
FROM
  global_temp.global_temp_view_latest_phones;

-- To query a Global Temporary View in a SELECT statement we have to use the "global_temp" database qualifier, which is infact a temporary database in the cluster 

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

SHOW VIEWS;

-- COMMAND ----------

SELECT
  *
FROM
  global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

DROP TABLE smartphones;

DROP VIEW view_apple_phones;
DROP VIEW global_temp.global_temp_view_latest_phones;

-- COMMAND ----------


