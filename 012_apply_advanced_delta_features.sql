-- Databricks notebook source
-- Time Travel
-- -----------

-- Audit data changes

-- DESCRIBE HISTORY command

-- Query older versions of the data
--    Using a timestamp
--    Using a version number

-- Rollback Versions
--    RESTORE TABLE command

-- COMMAND ----------

-- Compacting Small Files and Indexing
-- -----------------------------------

-- OPTIMIZE command

-- Co-locate column information
--    OPTIMIZE my_table ZORDER BY column_name

-- COMMAND ----------

-- Vacuum a Delta table
-- --------------------

-- Cleaning up unsed data files
--    uncommitted files
--    files that are no longer in latest table state

-- VACUUM command
--    Default retention period: 7 days

-- Vacuum = no time travel

-- COMMAND ----------



-- COMMAND ----------

DESCRIBE HISTORY employees;

-- COMMAND ----------

SELECT
  *
FROM
  employees VERSION AS OF 1;

-- COMMAND ----------

SELECT
  *
FROM
  employees @v1;

-- COMMAND ----------

DELETE FROM
  employees;
  
-- All data has been removed

-- COMMAND ----------

SELECT
  *
FROM
  employees;

-- COMMAND ----------

RESTORE TABLE employees VERSION AS OF 2;

-- COMMAND ----------

SELECT
  *
FROM
  employees;

-- COMMAND ----------

DESC HISTORY employees;

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- COMMAND ----------

OPTIMIZE employees ZORDER BY (id);

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- COMMAND ----------

DESC HISTORY employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VACUUM employees;

-- We need to specify a retention period and by default this retention period is 7 days

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS;

-- Error in SQL statement: IllegalArgumentException: requirement failed: Are you sure you would like to vacuum files with such a low retention period? If you have
-- writers that are currently writing to this table, there is a risk that you may corrupt the
-- state of your Delta table.

-- COMMAND ----------

SET
  spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

SELECT
  *
FROM
  employees VERSION AS OF 1;
  
-- FileReadException: Error while reading file dbfs:/user/hive/warehouse/employees/part-00000-e0b1c12b-fab0-4720-b4b4-354936cea981-c000.snappy.parquet. File dbfs:/user/hive/warehouse/employees/part-00000-e0b1c12b-fab0-4720-b4b4-354936cea981-c000.snappy.parquet referenced in the transaction log cannot be found. This occurs when data has been manually deleted from the file system rather than using the table `DELETE` statement. For more information, see https://docs.microsoft.com/azure/databricks/delta/delta-intro#frequently-asked-questions

-- COMMAND ----------

DROP TABLE employees;

-- COMMAND ----------

SELECT
  *
FROM
  employees;

-- Error in SQL statement: AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `employees` cannot be found. Verify the spelling and correctness of the schema and catalog.

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------


