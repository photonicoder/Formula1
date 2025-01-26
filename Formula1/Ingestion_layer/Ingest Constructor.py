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

constructors_schema ="constructorId INT, constructorRef STRING,name STRING, nationality STRING,url STRING"

# COMMAND ----------

Constructor_df=spark.read.schema(constructors_schema).json(f"{raw_path}/{v_file_date}/constructors.json")
Constructor_df.show(2)

# COMMAND ----------

Constructor_df=Constructor_df.withColumnsRenamed({'constructorId':'constructor_Id', 'constructorRef':'constructor_Ref','circuitId':"circuit_Id"})
Constructor_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

Constructor_df=Constructor_df.drop(col("url"))
Constructor_df.show(2)

# COMMAND ----------

#Constructor_df=Constructor_df.withColumn("Ingestion_date",current_timestamp())
#Constructor_df.show(1)



Constructor_df=add_ingestion_date(Constructor_df,v_data_source)
Constructor_df.show(1)

# COMMAND ----------

Constructor_df=Constructor_df.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

#Write It in parquet format
Constructor_df.write.mode("overwrite").parquet(f"{processed_path}d/Constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.constructor

# COMMAND ----------

Constructor_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.Constructor")

# COMMAND ----------

dbutils.notebook.exit("Success")