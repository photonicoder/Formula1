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

qualifying_schema=StructType([StructField("qualifyingId",IntegerType(),True),
                              StructField("raceId",IntegerType(),True),
                                StructField("driverId",IntegerType(),True),
                                StructField("constructorId",IntegerType(),True),
                                StructField("number",IntegerType(),True),
                                StructField("position",IntegerType(),True),
                                StructField("q1",StringType(),True),
                                StructField("q2",StringType(),True),
                                StructField("q3",StringType(),True)
                                ])

# COMMAND ----------

qualifying_schema_df=spark.read.schema(qualifying_schema).json(f"{raw_path}/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_schema_df.printSchema()

# COMMAND ----------

qualifying_schema_df=qualifying_schema_df.withColumnsRenamed({"raceId":"race_Id","driverId":"driver_Id","constructorId":"constructor_Id",'qualifyingId':'qualifying_Id'})
qualifying_schema_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#qualifying_schema_df=qualifying_schema_df.withColumn("Ingestion_date",current_timestamp())
#qualifying_schema_df.show(1)

# COMMAND ----------

qualifying_schema_df=add_ingestion_date(qualifying_schema_df,v_data_source)
qualifying_schema_df.show(1)

# COMMAND ----------

#Write It in parquet format
qualifying_schema_df.write.mode("overwrite").parquet(f"{processed_path}/qualifying")

# COMMAND ----------

qualifying_schema_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")