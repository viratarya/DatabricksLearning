# Databricks notebook source
spark.read.csv("dbfs:/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df = spark.read.csv("dbfs:/FileStore/tables/countries.csv")

# COMMAND ----------

type(countries_df)

# COMMAND ----------

countries_df.show()

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.csv("dbfs:/FileStore/tables/countries.csv",header= True)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.options(header=True).csv('/FileStore/tables/countries.csv')

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.schema

# COMMAND ----------

countries_df.describe

# COMMAND ----------

countries_df = spark.read.options(header=True,inferSchema=True).csv('/FileStore/tables/countries.csv')

# COMMAND ----------



# COMMAND ----------


