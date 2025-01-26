# Databricks notebook source
# Service Principal credentials
client_id = "f1b182d9-9305-4518-b2ea-eae6ee21721f"              # Application (client) ID from Azure AD
tenant_id = "97fd0735-3d63-479b-bf19-8bbe8d0e6ed2"              # Directory (tenant) ID from Azure AD
client_secret = "i6d8Q~YVRZ7na0UA4kzg6Iw9vl1s~IueDt6RdbKy"      # The client secret you generated
storage_account_name = "nishantdev1"  # Name of your Azure Storage account
container_name = "demo"    # Name of your container


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