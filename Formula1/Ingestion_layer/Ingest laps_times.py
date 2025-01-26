# Databricks notebook source
dbutils.widgets.text("p_data_source", "")   
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")   
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StringType, StructField,StructType,IntegerType,DoubleType,DateType,TimestampType,FloatType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

laps_times_schema=StructType([StructField("raceId",IntegerType(),True),
                          StructField("driverId",IntegerType(),True),
                          StructField("lap",IntegerType(),True),
                          StructField("position",IntegerType(),True),
                          StructField("time",StringType(),True),
                          StructField("milliseconds",IntegerType(),True)
                          ])

# COMMAND ----------

laps_times_df=spark.read.schema(laps_times_schema).csv(f"{raw_path}/{v_file_date}/laps_times")

# COMMAND ----------

laps_times_df.printSchema()

# COMMAND ----------

laps_times_df=laps_times_df.withColumnsRenamed({"raceId":"race_Id"})
laps_times_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#laps_times_df=laps_times_df.withColumn("Ingestion_date",current_timestamp())
laps_times_df=add_ingestion_date(laps_times_df,v_data_source)
laps_times_df.show(1)

# COMMAND ----------

#Write It in parquet format
laps_times_df.write.mode("overwrite").parquet(f"{processed_path}/laps_times")

# COMMAND ----------

laps_times_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.laps_times")

# COMMAND ----------

dbutils.notebook.exit("Success")