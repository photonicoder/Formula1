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

from pyspark.sql.types import StringType, StructField,StructType,IntegerType,DoubleType,DateType,TimestampType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

name_schema=StringType([StructField("forename",StringType(),True),
                        StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema=StringType([StructField("driverId",IntegerType(),True),
                          StructField("driverRef",StringType(),True),
                          StructField("name",name_schema),
                          StructField("dob",DateType(),True),
                          StructField("nationality",StringType(),True),
                          StructField("url",StringType(),True),
                          StructField("number",IntegerType(),True),
                          StructField("code",StringType(),True)])

# COMMAND ----------



# COMMAND ----------

drivers_df=spark.read.json(f"{raw_path}/{v_file_date}/drivers.json")
drivers_df.show(2)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

drivers_df=drivers_df.withColumnsRenamed({'driverId':'driver_Id', 'driverRef':'driver_Ref'})
drivers_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

drivers_df=drivers_df.drop(col("url"))
drivers_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import concat,lit,current_timestamp

# COMMAND ----------

#drivers_df=drivers_df.withColumn("Ingestion_date",current_timestamp())
drivers_df=add_ingestion_date(drivers_df,v_data_source)
drivers_df=drivers_df.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))
drivers_df.show(1)

# COMMAND ----------

drivers_df=drivers_df.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

#Write It in parquet format
drivers_df.write.mode("overwrite").parquet(f"{processed_path}/drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.drivers

# COMMAND ----------

drivers_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")