# Databricks notebook source
# MAGIC %run /Ingestion_layer/configuration

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_path}/circuits")
races_df=spark.read.parquet(f"{processed_path}/races")
circuits_df.show()
races_df.show()

# COMMAND ----------

circiut_races_df=circuits_df.join(races_df.circuit_Id==circuits_df.circuit_Id,how='ineer')

# COMMAND ----------

df=df.filter(df.year==2017 & df.round==1)
df.show()

# COMMAND ----------

