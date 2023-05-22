-- Databricks notebook source
-- CTAS
-- ----

-- CREATE TABLE _ AS SELECT statement

-- CREATE TABLE table_1
-- AS SELECT * FROM table_2

-- Automatically infer schema information from query results
--    Do not support manual schema declaration

-- COMMAND ----------

-- CTAS: Filtering and Renaming Columns
-- ------------------------------------

-- CREATE TABLE table_1
-- AS SELECT col_1, col_3 AS new_col_3 FROM table_2

-- COMMAND ----------

-- CTAS: Additional Options
-- ------------------------

-- CREATE TABLE new_table
--  COMMENT "Contains PII"
--  PARTITIONED BY (city, birth_date)
--  LOCATION '/some/path'
-- AS SELECT id, name, email, birth_date, city FROM users

-- COMMAND ----------



-- COMMAND ----------

-- TABLE CONSTRAINTS
-- -----------------

-- NOT NULL constraints
-- CHECK constraints

-- ALTER TABLE table_name ADD CONSTRAINT contraint_name constraint_details


-- You must ensure that there is no data violating the constraint is already in the table prior to defining the constraint

-- COMMAND ----------

-- ALTER TABLE orders ADD CONSTRAINT valid_date CHECK (date > '2020-01-01')

-- COMMAND ----------



-- COMMAND ----------

-- Cloning Delta Lake Table
-- ------------------------


-- DEEP CLONE
-- ----------
--    DEEP CLONE fully copies both data and metadata from a source table to a target

-- CREATE TABLE table_clone
-- DEEP CLONE source_table

-- This copy can occur incrementally
--    Executing this command again can synchronize changes from the source to the target location

-- Because all the data must be copied over, this can take a while for large dataset


-- SHALLOW CLONE
-- -------------
--    You can quickly create a copy of a table since it just copied the Delta Transaction logs
--    There is not data moving during Shallow Cloning

-- CREATE TABLE table_clone
-- SHALLOW CLONE source_table

-- It is a good option to test out applying changes on a table without the risk of modifying a current table

-- COMMAND ----------

-- Cloning is a great way to copy Production tables for testing your code in Development.
-- In either cases, DEEP or SHALLOW, data modification applied to the cloned version of the table will be tracked and stored separately from the source. So, it will not affect the source table.

-- COMMAND ----------


