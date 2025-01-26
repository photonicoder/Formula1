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

from pyspark.sql.types import StringType, StructField,StructType,IntegerType,DoubleType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuit_schema = StructType([StructField("circuitId", IntegerType(), True),
                             StructField("circuitRef", StringType(), True),
                             StructField("name", StringType(), True),
                             StructField("location", StringType(), True),
                             StructField("country", StringType(), True),
                             StructField("lat", DoubleType(), True),
                             StructField("lng", DoubleType(), True),
                             StructField("alt", IntegerType(), True),
                             StructField("url", StringType(), True)
                             ])

# COMMAND ----------

path=f"{raw_path}/{v_file_date}/circuits.csv"
df=spark.read.option('header',True).schema(circuit_schema).csv(path)
df.show(2)

# COMMAND ----------

df=df.withColumnsRenamed({'circuitId':'circuit_id', 'lat':'latitude','circuitRef':"circuitRef",'lng':'longtitude','alt':'alttitude'})
df.show(2)

# COMMAND ----------

df=df.drop('url')


# COMMAND ----------

df.printSchema()

# COMMAND ----------

#df_final=df.withColumn("Ingestion_date",current_timestamp())
#df_final.show(1)

df_final=add_ingestion_date(df,v_data_source)
df_final=df_final.withColumn("file_date",lit(v_file_date))
df_final.show(1)

# COMMAND ----------

#Write It in parquet format
df_final.write.mode("overwrite").parquet(f"{processed_path}/circuits")

# COMMAND ----------

df_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")