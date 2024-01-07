# Databricks notebook source
# MAGIC %md
# MAGIC Register an Azure AD application to create a service principal
# MAGIC
# MAGIC Application Client ID
# MAGIC Application Tenant ID 
# MAGIC Client Secret
# MAGIC
# MAGIC
# MAGIC Give the Service Principal 'Contributer' access to ADLS
# MAGIC
# MAGIC Mount ADLS to DBFS using the ID's and secret
# MAGIC

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "applicationId",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="databrics-secrets-va0209",key="secretsValue"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/$tenantId/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.secrets.get(scope="databrics-secrets-va0209",key="tenantId")

# COMMAND ----------

for i in dbutils.secrets.get(scope="databrics-secrets-va0209",key="tenantId"):
    print(i)

# COMMAND ----------

application_id = dbutils.secrets.get(scope="databrics-secrets-va0209",key="applicationId")
tenant_id = dbutils.secrets.get(scope="databrics-secrets-va0209",key="tenantId")
secret = dbutils.secrets.get(scope="databrics-secrets-va0209",key="secretsValue")
mount_point ='/mnt/bronze'

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@datalakeva0209.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------


