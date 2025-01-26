-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw


-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/nishantdev1/raw/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StringType, StructField,StructType,IntegerType,DoubleType
-- MAGIC from pyspark.sql.functions import current_timestamp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuit_schema = StructType([StructField("circuitId", IntegerType(), True),
-- MAGIC                              StructField("circuitRef", StringType(), True),
-- MAGIC                              StructField("name", StringType(), True),
-- MAGIC                              StructField("location", StringType(), True),
-- MAGIC                              StructField("country", StringType(), True),
-- MAGIC                              StructField("lat", DoubleType(), True),
-- MAGIC                              StructField("lng", DoubleType(), True),
-- MAGIC                              StructField("alt", IntegerType(), True),
-- MAGIC                              StructField("url", StringType(), True)
-- MAGIC                              ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC path=f"{raw_path}/circuits.csv"
-- MAGIC df=spark.read.option('header',True).schema(circuit_schema).csv(path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").saveAsTable("f1_raw.circuits_pythonss")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE  f1_raw.circuits
AS
SELECT * FROM f1_raw.circuits_pythonss


-- COMMAND ----------

DROP TABLE f1_raw.circuits_pythonss;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_schema = StructType([StructField("raceId", IntegerType(), True),
-- MAGIC                              StructField("year", StringType(), True),
-- MAGIC                              StructField("round", StringType(), True),
-- MAGIC                              StructField("circuitId", IntegerType(), True),
-- MAGIC                              StructField("name", StringType(), True),
-- MAGIC                              StructField("date", StringType(), True),
-- MAGIC                              StructField("time", StringType(), True),
-- MAGIC                              StructField("url", StringType(), True)
-- MAGIC                              ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC path=f"{raw_path}/races.csv"
-- MAGIC df_1=spark.read.option('header',True).schema(races_schema).csv(path)
-- MAGIC df_1.show(1)

-- COMMAND ----------

DROP TABLE f1_raw.races;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_1.write.format("delta").saveAsTable("f1_raw.races")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Creatot constructors table**
-- MAGIC
-- MAGIC **.Single Line JSON**
-- MAGIC
-- MAGIC **.Simple structure**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC constructors_schema ="constructorId INT, constructorRef STRING,name STRING, nationality STRING,url STRING"
-- MAGIC Constructor_df=spark.read.schema(constructors_schema).json(f"{raw_path}/constructors.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC Constructor_df.write.format("delta").saveAsTable("f1_raw.constructors") 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Creator drivers table**
-- MAGIC
-- MAGIC **.Single Line json**
-- MAGIC
-- MAGIC **complex struture**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC name_schema=StringType([StructField("forename",StringType(),True),
-- MAGIC                         StructField("surname",StringType(),True)])
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StringType, StructField,StructType,IntegerType,DoubleType,DateType,TimestampType,FloatType

-- COMMAND ----------

-- MAGIC %python
-- MAGIC driver_schema=StringType([StructField("driverId",IntegerType(),True),
-- MAGIC                           StructField("driverRef",StringType(),True),
-- MAGIC                           StructField("name",name_schema),
-- MAGIC                           StructField("dob",DateType(),True),
-- MAGIC                           StructField("nationality",StringType(),True),
-- MAGIC                           StructField("url",StringType(),True),
-- MAGIC                           StructField("number",IntegerType(),True),
-- MAGIC                           StructField("code",StringType(),True)])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_df=spark.read.json(f"{raw_path}/drivers.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_df.write.format("delta").saveAsTable("f1_raw.drivers") 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Creating RESULT table**
-- MAGIC
-- MAGIC **Single Line Json**
-- MAGIC
-- MAGIC **Simple struture**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_schema=StructType([StructField("resultId",IntegerType(),False),
-- MAGIC                           StructField("raceId",IntegerType(),True),
-- MAGIC                           StructField("driverId",IntegerType(),True),
-- MAGIC                           StructField("constructorId",IntegerType(),True),
-- MAGIC                           StructField("number",IntegerType(),True),
-- MAGIC                           StructField("grid",IntegerType(),True),
-- MAGIC                           StructField("position",IntegerType(),True),
-- MAGIC                           StructField("positionText",StringType(),True),
-- MAGIC                           StructField("positionOrder",IntegerType(),True),
-- MAGIC                           StructField("points",FloatType(),True),
-- MAGIC                           StructField("laps",IntegerType(),True),
-- MAGIC                           StructField("time",StringType(),True),
-- MAGIC                           StructField("milliseconds",IntegerType(),True),
-- MAGIC                           StructField("fastestLap",IntegerType(),True),
-- MAGIC                           StructField("rank",IntegerType(),True),
-- MAGIC                           StructField("fastestLapTime",StringType(),True),
-- MAGIC                           StructField("fastestLapSpeed",FloatType(),True),
-- MAGIC                           StructField("statusId",IntegerType(),True)
-- MAGIC                           ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df=spark.read.schema(results_schema).json(f"{raw_path}/results.json",schema=results_schema)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").saveAsTable("f1_raw.results") 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE LAPS TIME FOLDER
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC laps_times_schema=StructType([StructField("raceId",IntegerType(),True),
-- MAGIC                           StructField("driverId",IntegerType(),True),
-- MAGIC                           StructField("lap",IntegerType(),True),
-- MAGIC                           StructField("position",IntegerType(),True),
-- MAGIC                           StructField("time",StringType(),True),
-- MAGIC                           StructField("milliseconds",IntegerType(),True)
-- MAGIC                           ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC laps_times_df=spark.read.schema(laps_times_schema).csv(f"{raw_path}/laps_times")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.laps_times;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC laps_times_df.write.format("delta").saveAsTable("f1_raw.laps_times") 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC qualifying_schema=StructType([StructField("qualifyingId",IntegerType(),True),
-- MAGIC                               StructField("raceId",IntegerType(),True),
-- MAGIC                                 StructField("driverId",IntegerType(),True),
-- MAGIC                                 StructField("constructorId",IntegerType(),True),
-- MAGIC                                 StructField("number",IntegerType(),True),
-- MAGIC                                 StructField("position",IntegerType(),True),
-- MAGIC                                 StructField("q1",StringType(),True),
-- MAGIC                                 StructField("q2",StringType(),True),
-- MAGIC                                 StructField("q3",StringType(),True)
-- MAGIC                                 ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC qualifying_schema_df=spark.read.schema(qualifying_schema).json(f"{raw_path}/qualifying")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC qualifying_schema_df.write.format("delta").saveAsTable("f1_raw.qualifying") 