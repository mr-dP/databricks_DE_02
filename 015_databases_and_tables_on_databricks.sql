-- Databricks notebook source
CREATE TABLE managed_default
(width INT, length INT, height INT);
-- It is a Managed Table, since did not specify the "LOCATION" keyword during the creation of the table

INSERT INTO
  managed_default
VALUES(3, 2, 1);

-- COMMAND ----------

DESCRIBE EXTENDED managed_default;
-- Type       MANAGED
-- Location   dbfs:/user/hive/warehouse/managed_default
-- Provider   delta

-- COMMAND ----------

CREATE TABLE external_default
(width INT, length INT, height INT)
LOCATION 'dbfs:/FileStore/external_default';

INSERT INTO
  external_default
VALUES(3 INT, 2 INT, 1 INT);

-- EXTERNAL TABLE

-- COMMAND ----------

DESCRIBE EXTENDED external_default;
-- Type       EXTERNAL
-- Location   dbfs:/FileStore/external_default
-- Provider   delta

-- COMMAND ----------

DROP TABLE managed_default;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/managed_default

-- COMMAND ----------

DROP TABLE external_default;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/external_default

-- COMMAND ----------

CREATE SCHEMA new_default;

-- COMMAND ----------

DESC DATABASE EXTENDED new_default;
-- Location     dbfs:/user/hive/warehouse/new_default.db

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
(width INT, length INT, height INT);
-- It is a Managed Table, since did not specify the "LOCATION" keyword during the creation of the table

INSERT INTO
  managed_new_default
VALUES(3, 2, 1);

------------------------------------------------------------------

CREATE TABLE external_new_default
(width INT, length INT, height INT)
LOCATION 'dbfs:/FileStore/external_new_default';

INSERT INTO
  external_new_default
VALUES(3 INT, 2 INT, 1 INT);


-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default;
-- Type        MANAGED
-- Location    dbfs:/user/hive/warehouse/new_default.db/managed_new_default
-- Provider    delta

-- COMMAND ----------

DESCRIBE EXTENDED external_new_default;
-- Type        EXTERNAL
-- Location    dbfs:/FileStore/external_new_default
-- Provider    delta

-- COMMAND ----------

DROP TABLE external_new_default;
DROP TABLE managed_new_default;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/new_default.db/managed_new_default

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/external_new_default

-- COMMAND ----------

CREATE SCHEMA custom LOCATION 'dbfs:/FileStore/schemas/custom.db';

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom;
-- Location     dbfs:/FileStore/schemas/custom.db

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom
(width INT, length INT, height INT);

INSERT INTO managed_custom VALUES (3 INT, 2 INT, 1 INT);

---------------------------------------

CREATE TABLE external_custom
(width INT, length INT, height INT)
LOCATION 'dbfs:/FileStore/external_custom';

INSERT INTO external_custom VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom;
-- Type       MANAGED
-- Location   dbfs:/FileStore/schemas/custom.db/managed_custom
-- Provider   delta

-- COMMAND ----------

DESCRIBE EXTENDED external_custom;
-- Type       EXTERNAL
-- Location   dbfs:/FileStore/external_custom
-- Provider   delta

-- COMMAND ----------

DROP TABLE managed_custom;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/schemas/custom.db/managed_custom

-- COMMAND ----------

DROP TABLE external_custom;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/external_custom

-- COMMAND ----------


