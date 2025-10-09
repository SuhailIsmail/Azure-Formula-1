# Databricks notebook source
# MAGIC %md
# MAGIC # Access using Service principle and app

# COMMAND ----------

def mount_adls(Storage, container):
    client_id = dbutils.secrets.get(scope = 'Scop1',key='client')
    tenant_id = dbutils.secrets.get(scope = 'Scop1',key='tenant')
    secret_val = dbutils.secrets.get(scope = 'Scop1',key='secret')

    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret":secret_val,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{Storage}/{container}" for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(f"/mnt/{Storage}/{container}")

    dbutils.fs.mount(
    source = f"abfss://{container}@{Storage}.dfs.core.windows.net/",
    mount_point = f"/mnt/{Storage}/{container}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('stprac1','raw')

# COMMAND ----------

mount_adls('stprac1','processed')

# COMMAND ----------

mount_adls('stprac1','presentation')

# COMMAND ----------

display(dbutils.fs.mounts())