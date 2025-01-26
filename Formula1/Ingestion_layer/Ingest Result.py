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

results_schema=StructType([StructField("resultId",IntegerType(),False),
                          StructField("raceId",IntegerType(),True),
                          StructField("driverId",IntegerType(),True),
                          StructField("constructorId",IntegerType(),True),
                          StructField("number",IntegerType(),True),
                          StructField("grid",IntegerType(),True),
                          StructField("position",IntegerType(),True),
                          StructField("positionText",StringType(),True),
                          StructField("positionOrder",IntegerType(),True),
                          StructField("points",FloatType(),True),
                          StructField("laps",IntegerType(),True),
                          StructField("time",StringType(),True),
                          StructField("milliseconds",IntegerType(),True),
                          StructField("fastestLap",IntegerType(),True),
                          StructField("rank",IntegerType(),True),
                          StructField("fastestLapTime",StringType(),True),
                          StructField("fastestLapSpeed",FloatType(),True),
                          StructField("statusId",IntegerType(),True)
                          ])

# COMMAND ----------

results_df=spark.read.schema(results_schema).json(f"{raw_path}/{v_file_date}/results.json",schema=results_schema)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

results_df=results_df.withColumnsRenamed({"raceId":"race_Id","constructorId":"constructor_Id","resultId":"result_Id","positionText":"position_Text","positionOrder":"position_Order","driverId":"driver_Id","fastestLap":"fastest_Lap","fastestLapTime":"fastest_Lap_Time","fastestLapSpeed":"fastest_Lap_Speed"
                                          
                                          })
results_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_df=results_df.drop(col("statusId"))
results_df.show(2)

# COMMAND ----------


results_df=add_ingestion_date(results_df,v_data_source)
results_df=results_df.withColumn("file_date",lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC **Mthod 1**

# COMMAND ----------

# for race_id_list in results_df.select("race_Id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"DELETE FROM f1_processed.results WHERE race_Id = {race_id_list.race_Id}")

# COMMAND ----------

# MAGIC %md
# MAGIC Metdod 1

# COMMAND ----------

# spark.conf.set("spark.sql.source.partitionOverwriteMode", "dynamic")


# COMMAND ----------

overwrite_partition(results_df, "f1_processed","results", "race_Id")

# COMMAND ----------

#Write It in parquet format
# Assuming you're using Delta Lake and have delta format enabled
results_df.write.format("delta").mode("append").partitionBy("race_Id").save(f"{processed_path}/delta_results")


# COMMAND ----------

results_df.printSchema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_Id,count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_Id ORDER BY race_Id DESC;

# COMMAND ----------



# COMMAND ----------

dbutils.notebook.exit("Success")