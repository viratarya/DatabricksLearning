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

from pyspark.sql.functions import *

# COMMAND ----------

countries = countries.withColumn('timestamp',current_timestamp())

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.schema

# COMMAND ----------

countries.select(month(countries['timestamp'])).display()

# COMMAND ----------

from pyspark.sql.functions import *



countries = countries.withColumn('date_literal',lit('22-10-2022'))

# COMMAND ----------

countries = countries.withColumn('date',to_date(countries['date_literal'],'dd-MM-yyyy'))

countries = countries.withColumn('dayOfTheYear',dayofyear('date'))


# COMMAND ----------

countries.display()

# COMMAND ----------

countries.select(dayofyear('date')).display()

# COMMAND ----------

countries.filter(countries['population']> 1000000).display()

# COMMAND ----------

from pyspark.sql.functions import locate

# COMMAND ----------

countries.filter(locate("B",countries['capital'])==1).display() 

# COMMAND ----------

countries.filter((locate("B",countries['capital'])==1) | (countries['population']>100000000) ).display()

# COMMAND ----------

countries.filter((length("name")>= 15) & (countries['region_id']!= 10) ).display()

# COMMAND ----------

countries.filter("length(name)> 15 and region_id !=10").display()

# COMMAND ----------


