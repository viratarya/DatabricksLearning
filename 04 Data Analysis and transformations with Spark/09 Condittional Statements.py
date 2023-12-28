# Databricks notebook source
countries_path = 'dbfs:/FileStore/tables/countries.csv'

df = spark.read.options(header=True).options(inferSchema= True).csv('dbfs:/FileStore/tables/countries.csv')

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType

Countries_schema = StructType([StructField('COUNTRY_ID', IntegerType(), True), 
            StructField('NAME', StringType(), True),
            StructField('NATIONALITY', StringType(), True), 
            StructField('COUNTRY_CODE', StringType(), True),
            StructField('ISO_ALPHA2', StringType(), True), 
            StructField('CAPITAL', StringType(), True),
            StructField('POPULATION', IntegerType(), True), 
            StructField('AREA_KM2', DoubleType(), True),
            StructField('REGION_ID', IntegerType(), True), 
            StructField('SUB_REGION_ID', IntegerType(), True),
            StructField('INTERMEDIATE_REGION_ID', IntegerType(), True),
            StructField('ORGANIZATION_REGION_ID', IntegerType(), True)])

countries= spark.read.csv(path=countries_path,header=True, schema=Countries_schema)

# COMMAND ----------

countries.display()


# COMMAND ----------

from pyspark.sql.functions import when
countries.withColumn('name_length',when(countries['population'] > 1000000,'large')\
    .otherwise('not large')).display()

# COMMAND ----------


