# Databricks notebook source
# Service Principal credentials
client_id = "f1b182d9-9305-4518-b2ea-eae6ee21721f"              # Application (client) ID from Azure AD
tenant_id = "97fd0735-3d63-479b-bf19-8bbe8d0e6ed2"              # Directory (tenant) ID from Azure AD
client_secret = "i6d8Q~YVRZ7na0UA4kzg6Iw9vl1s~IueDt6RdbKy"      # The client secret you generated
storage_account_name = "nishantdev1"  # Name of your Azure Storage account
container_name = "demo"    # Name of your container


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@nishantdev1.dfs.core.windows.net/",
  mount_point = "/mnt/nishantdev1/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/nishantdev1/demo"))

# COMMAND ----------

df=spark.read.csv("/mnt/nishantdev1/demo/races.csv")
df.show()