# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.datalakeva0209.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalakeva0209.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalakeva0209.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-01-07T00:30:00Z&st=2024-01-06T16:30:00Z&spr=https&sig=ZvAt%2BHNyLTA%2Fr7K7oPCoCyQtX%2BGK%2BzgQEUmCE3JX0IQ%3D")

# COMMAND ----------

countries= spark.read.csv("abfss://bronze@datalakeva0209.dfs.core.windows.net/countries.csv",header=True)
regions = spark.read.csv("abfss://bronze@datalakeva0209.dfs.core.windows.net/country_regions.csv",header=True)

# COMMAND ----------

display(countries)
display(regions)

# COMMAND ----------


