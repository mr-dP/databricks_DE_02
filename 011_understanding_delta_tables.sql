-- Databricks notebook source
CREATE TABLE employees
-- USING DELTA
(id INT, name STRING, salary DOUBLE);

-- Delta is the default format and you do not need to specify the keyword "USING DELTA"

-- COMMAND ----------

INSERT INTO
  employees
VALUES
  (1, "Anil", 3200.0),
  (2, "Sameer", 4020.5),
  (3, "Jugnu", 2999.3),
  (4, "Tina", 4000.3),
  (5, "Anuj", 2500.0),
  (6, "Kush", 6200.3);

-- COMMAND ----------

SELECT
  *
FROM
  employees;

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- command to explore table metadata

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/employees"))

-- COMMAND ----------

UPDATE
  employees
SET
  salary = salary + 1
WHERE
  name LIKE 'A%';

-- COMMAND ----------

SELECT
  *
FROM
  employees;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/employees"))
-- MAGIC
-- MAGIC # Rather than updating the records in the files itself, we make a copy of them and later Delta Lake use the transaction log to indicate which files are valid in the current version of the table

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- command to explore table metadata

-- COMMAND ----------

DESCRIBE HISTORY employees;

-- Since the transaction log contains all the change to the Delta Lake table we can easily review the table history

-- COMMAND ----------

-- # The transaction log is location under the "_delta_log" folder in the table directory
-- # Each transaction is a new JSON file being written to the Delta Lake transaction log

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json'

-- COMMAND ----------


