# Databricks notebook source
print('xyz')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Header
# MAGIC ## Sub header
# MAGIC ### sub sub header

# COMMAND ----------

print("Hello World")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.nyctaxi.trips;

# COMMAND ----------

print("Hello")

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Hello World")

# COMMAND ----------

#DBUTILS


# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/'

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/tables/')

# COMMAND ----------



# COMMAND ----------


