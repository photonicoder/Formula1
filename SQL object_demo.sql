-- Databricks notebook source
CREATE DATABASE demo

-- COMMAND ----------

CREATE DATABASE if NOT EXISTS demo

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

use demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Manage Table Spark

-- COMMAND ----------

-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df=spark.read.parquet(f"{presenattion_path}/race_result")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("delta").saveAsTable("demo.race_result_pythonss")
-- MAGIC

-- COMMAND ----------

drop table default.race_result_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using SQL
-- MAGIC

-- COMMAND ----------

CREATE TABLE  demo.race_result_sql
AS
SELECT * FROM demo.race_result_python


-- COMMAND ----------

select * from demo.race_result_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #  for external table you need to use specify the path and in this table is drop from hive store but available at external postion
-- MAGIC
-- MAGIC **Learning Objectives**
-- MAGIC
-- MAGIC **1.Create External Table using python**
-- MAGIC
-- MAGIC **2.Create External Table using sql**
-- MAGIC

-- COMMAND ----------

