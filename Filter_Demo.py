# Databricks notebook source
# MAGIC %run /Ingestion_layer/configuration

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_path}/circuits")
circuits_df.show()

# COMMAND ----------

df=df.filter(df.year==2017 & df.round==1)
df.show()

# COMMAND ----------

