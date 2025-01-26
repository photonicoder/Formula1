# Databricks notebook source
client_id= dbutils.secrets.get(scope='formula', key='formula1-client-id')
tenant_id=dbutils.secrets.get(scope='formula', key='formuala1tenatid')
client_secret=dbutils.secrets.get(scope='formula', key='formula1clientsecret')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.nishantdev1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nishantdev1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nishantdev1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.nishantdev1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nishantdev1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@nishantdev1.dfs.core.windows.net/"))

# COMMAND ----------

df=spark.read.csv("abfss://demo@nishantdev1.dfs.core.windows.net/races.csv")
df.show()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula')

# COMMAND ----------

dbutils.secrets.get(scope='formula', key='formula-dl-acess-key')

# COMMAND ----------

