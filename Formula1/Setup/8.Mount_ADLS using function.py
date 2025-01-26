# Databricks notebook source
client_id= dbutils.secrets.get(scope='formula', key='formula1-client-id')
tenant_id=dbutils.secrets.get(scope='formula', key='formuala1tenatid')
client_secret=dbutils.secrets.get(scope='formula', key='formula1clientsecret')

# COMMAND ----------

def mounnt_conatiners(stroage_accont, container_name):
    
    client_id= dbutils.secrets.get(scope='formula', key='formula1-client-id')
    tenant_id=dbutils.secrets.get(scope='formula', key='formuala1tenatid')
    client_secret=dbutils.secrets.get(scope='formula', key='formula1clientsecret')
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    if any(mount.mountPoint == f"/mnt/{stroage_accont}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{stroage_accont}/{container_name}")
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{stroage_accont}.dfs.core.windows.net/",
        mount_point = f"/mnt/{stroage_accont}/{container_name}",
        extra_configs = configs)
    display(dbutils.fs.mounts())

    

# COMMAND ----------

mounnt_conatiners("nishantdev1", "raw")

# COMMAND ----------

mounnt_conatiners("nishantdev1", "presenattion")

# COMMAND ----------

mounnt_conatiners("nishantdev1", "processed")