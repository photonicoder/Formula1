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

pit_stops_schema=StructType([StructField("raceId",IntegerType(),True),
                          StructField("driverId",IntegerType(),True),
                          StructField("stop",IntegerType(),True),
                          StructField("lap",IntegerType(),True),
                          StructField("time",StringType(),True),
                          StructField("duration",IntegerType(),True),
                          StructField("milliseconds",IntegerType(),True)
                          ])

# COMMAND ----------

pit_stops_df=spark.read.schema(pit_stops_schema).option("multiline","true").json(f"{raw_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

pit_stops_df.printSchema()

# COMMAND ----------

pit_stops_df=pit_stops_df.withColumnsRenamed({"raceId":"race_Id","driverId":"driver_Id" })
pit_stops_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#pit_stops_df=pit_stops_df.withColumn("Ingestion_date",current_timestamp())
pit_stops_df=add_ingestion_date(pit_stops_df,v_data_source)
pit_stops_df.show(1)

# COMMAND ----------

#Write It in parquet format
pit_stops_df.write.mode("overwrite").parquet(f"{processed_path}/pit_stops")

# COMMAND ----------

pit_stops_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")