# Databricks notebook source


# COMMAND ----------

formual1_account_key=dbutils.secrets.get(scope='formula', key='formula-dl-acess-key')

# COMMAND ----------

storage_account_name = "nishantdev1"
storage_account_access_key = "+iPb9lPQuvKShosaGMlRgu2qfaI4ml10FV/4J6jchN3D83jkKajZeHT1Gpmgno5v4Fato56he0Hh+AStHslxNw=="


# COMMAND ----------

# Set the configuration
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", formual1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@nishantdev1.dfs.core.windows.net/"))

# COMMAND ----------

df=spark.read.csv("abfss://demo@nishantdev1.dfs.core.windows.net/races.csv")
df.show()

# COMMAND ----------

