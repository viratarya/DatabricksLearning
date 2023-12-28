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

countries.groupBy('region_id')

# COMMAND ----------

countries.groupBy('region_id')



# COMMAND ----------

from pyspark.sql.functions import *
countries.groupBy('region_id').sum('population').display()

# COMMAND ----------

countries. \
groupBy('region_id'). \
min('population'). \
display()
    

# COMMAND ----------

countries. \
groupBy('region_id'). \
avg('population'). \
display()

# COMMAND ----------

countries. \
groupBy('region_id'). \
sum('population','area_km2'). \
display()

# COMMAND ----------

from pyspark.sql.functions import *
countries. \
groupBy('region_id'). \
agg(avg('population'),sum('area_km2')). \
display()

# COMMAND ----------

from pyspark.sql.functions import *
countries. \
groupBy('region_id','sub_region_id'). \
agg(avg('population'),sum('area_km2')). \
display()

# COMMAND ----------

from pyspark.sql.functions import *
countries. \
groupBy('region_id','sub_region_id'). \
agg(avg('population').alias('avg_population') ,sum('area_km2').alias('total_area') ). \
display()

# COMMAND ----------

countries. \
groupBy('region_id','sub_region_id'). \
agg(sum('population') ,sum('area_km2') ). \
withColumnRenamed('sum(population)' , 'total_population'). \
withColumnRenamed('sum(area_km2)','total_area'). \
display()

# COMMAND ----------

from pyspark.sql.functions import asc
countries. \
groupBy('region_id','sub_region_id'). \
agg(max('population').alias('max_pop') ,min('population').alias('min_pop') ). \
orderBy(asc ('region_id')).\
display()

# COMMAND ----------


