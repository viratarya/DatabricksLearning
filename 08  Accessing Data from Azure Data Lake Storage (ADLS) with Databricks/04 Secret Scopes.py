# Databricks notebook source
application_id =
tenant_id =
secret = 


# COMMAND ----------

container_name ="bronze"
account_name = "datalakeva0209"
mount_point"/mnt/bronze"

# COMMAND ----------

# dbutils.fs.unmount('/mnt/bronze')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakeva0209.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope>", key=AccessKey))

# COMMAND ----------

# uri Scheme
#abfss://bronze@datalakeva0209.dfs.core.windows.net/<path>/<path>/<file_name>
spark.read.csv("abfss://bronze@datalakeva0209.dfs.core.windows.net/countries.csv").display()

# COMMAND ----------

# storage-account= 

spark.conf.set(f"fs.azure.account.auth.type.$storage-account.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.$storage-account.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.$storage-account.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key=sasToken))
