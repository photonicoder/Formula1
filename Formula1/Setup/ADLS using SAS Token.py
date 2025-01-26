# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.nishantdev1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.nishantdev1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.nishantdev1.dfs.core.windows.net","sp=rl&st=2024-09-10T14:51:45Z&se=2024-09-10T22:51:45Z&spr=https&sv=2022-11-02&sr=c&sig=L%2B7VJnuAyFR8Du8w6t16IOj85FgLUI5jUvR7BEJtmJ0%3D")

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@nishantdev1.dfs.core.windows.net/"))

# COMMAND ----------

df=spark.read.csv("abfss://demo@nishantdev1.dfs.core.windows.net/races.csv")
df.show()