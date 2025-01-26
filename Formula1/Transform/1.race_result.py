# Databricks notebook source
dbutils.widgets.text("p_file_date", "")   
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_function"

# COMMAND ----------

processed_path

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_path}/race").withColumnRenamed('name','race_name').withColumnRenamed('date','race_date')
races_df.show(2)

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_path}/drivers").withColumnRenamed('number','driver_number').withColumnRenamed('name','driver_name'). withColumnRenamed('nationality','driver_nationality')


# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_path}/circuits").withColumnRenamed('location','circuits_location')
circuits_df.show(2)

# COMMAND ----------

Constructor_df=spark.read.parquet(f"{processed_path}/Constructor").withColumnRenamed("name","team")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_path}/results").filter(f"file_date='{v_file_date}'").withColumnRenamed('time','race_time').withColumnRenamed("race_Id","race_IdS")

# COMMAND ----------

#Joining circuit and races Data frames Api
races_cicuit_df=races_df.join(circuits_df,races_df.circuit_Id==circuits_df.circuit_id,"inner")\
    .select(races_df.race_Id,races_df.race_date,races_df.race_year,races_df.race_name,circuits_df.circuits_location)

# COMMAND ----------

race_result_df=results_df.join(races_cicuit_df,results_df.race_IdS==races_cicuit_df.race_Id)\
    .join(drivers_df,results_df.driver_Id==drivers_df.driver_Id)\
    .join(Constructor_df,Constructor_df.constructor_Id==results_df.constructor_Id)

# COMMAND ----------

race_result_df.select('race_Id')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=race_result_df.select('race_Id','race_name','race_year','race_date','driver_name','driver_nationality','driver_number','team','grid','race_time','points','fastest_lap','position_Order')\
    .withColumn('created_date',current_timestamp())

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{presenattion_path}/race_result")

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_presenation.race_result")
overwrite_partition(final_df, "f1_presenation","race_result", "race_Id")

# COMMAND ----------

race_result_df=spark.read.parquet(f"{presenattion_path}/race_result")
race_result_df.show(1)