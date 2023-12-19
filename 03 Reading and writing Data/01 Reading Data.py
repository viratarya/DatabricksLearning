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

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField


countries_schema= StructType([StructField('COUNTRY_ID', IntegerType(), False),
            StructField('NAME', StringType(), False),
            StructField('NATIONALITY', StringType(), False),
            StructField('COUNTRY_CODE', StringType(), False),
            StructField('ISO_ALPHA2', StringType(), False),
            StructField('CAPITAL', StringType(), False),
            StructField('POPULATION', IntegerType(), False),
            StructField('AREA_KM2', DoubleType(), False),
            StructField('REGION_ID', IntegerType(), True),
            StructField('SUB_REGION_ID', IntegerType(), True),
            StructField('INTERMEDIATE_REGION_ID',IntegerType(), True),
            StructField('ORGANIZATION_REGION_ID',IntegerType(), True)])

# COMMAND ----------

countries_df = spark.read.options(header=True).schema(countries_schema).csv('/FileStore/tables/countries.csv')

# COMMAND ----------

countries_sl_json = spark.read.json('dbfs:/FileStore/tables/countries_single_line.json')

# COMMAND ----------

display(countries_sl_json)

# COMMAND ----------

countries_ml_json = spark.read.options(multiLine = True).json('dbfs:/FileStore/tables/countries_multi_line.json')

# COMMAND ----------



# COMMAND ----------

display(countries_ml_json)

# COMMAND ----------

countries_txt = spark.read.csv('dbfs:/FileStore/tables/countries.txt',header = True, sep ='\t')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


