# Databricks notebook source


# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakeva0209.dfs.core.windows.net",
    "wlipf3eFjvArKy0OBpkncrCOXtYscfRaSLJW6NJGXnoQgZGIKF+4LbSzq+9gCzj6L/z4g8byWgCO+AStYATjbw==")

# COMMAND ----------

contries =spark.read.csv ("abfss://bronze@datalakeva0209.dfs.core.windows.net/countries.csv",header=True)
regions = spark.read.csv ("abfss://bronze@datalakeva0209.dfs.core.windows.net/country_regions.csv",header=True)

# COMMAND ----------

display(contries)
display(regions)
