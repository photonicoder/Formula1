# Databricks notebook source
dbutils.widgets.text("p_data_source", "")   
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")   
v_file_datee=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_datee

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StringType, StructField,StructType,IntegerType,DoubleType,DateType,TimestampType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_schema = StructType([StructField("raceId", IntegerType(), True),
                             StructField("year", StringType(), True),
                             StructField("round", StringType(), True),
                             StructField("circuitId", IntegerType(), True),
                             StructField("name", StringType(), True),
                             StructField("date", StringType(), True),
                             StructField("time", StringType(), True),
                             
                             StructField("url", StringType(), True)
                             ])

# COMMAND ----------

path=f"{raw_path}/{v_file_datee}/races.csv"
df=spark.read.option('header',True).schema(races_schema).csv(path)
df.show(1)

# COMMAND ----------

df=df.withColumnsRenamed({'raceId':'race_Id', 'year':'race_year','circuitId':"circuit_Id"})
df.show(2)

# COMMAND ----------

df=df.drop('url')
df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------


Race_final=add_ingestion_date(df,v_data_source)
Race_final=Race_final.withColumn("file_date",lit(v_file_datee))
Race_final.show(2)

# COMMAND ----------

Race_final.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_path}/race")


# COMMAND ----------

Race_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed.race")

# COMMAND ----------

dbutils.notebook.exit("Success")