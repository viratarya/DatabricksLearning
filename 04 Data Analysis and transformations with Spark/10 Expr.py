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

from pyspark.sql.functions  import expr
countries.select(expr('NAME as country_name')).display()

# COMMAND ----------

countries.select(expr('left(NAME,2) as nameInitials')).display()

# COMMAND ----------

countries= countries.withColumn('nameInitials', expr('upper(left(NAME,2))'))

# COMMAND ----------

countries.display()

# COMMAND ----------

countries = countries.withColumn('population_category', expr("case when population > 10000000 then 'very large' \
                                                              when population > 5000000   then 'Medium' \
                                                                  else ('small') end"))

# COMMAND ----------

countries = countries.withColumn('area_class', expr("case when area_km2 > 1000000 then 'very large' \
                                                              when area_km2 > 300000   then 'Medium' \
                                                                  else ('small') end"))

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.display()

# COMMAND ----------

countries_2 = countries.select('name','capital','population' )

# COMMAND ----------

countries_3 = countries.drop(countries['organization_region_id'])

# COMMAND ----------

countries_3.display()


# COMMAND ----------

countries_3.drop('sub_region_id','INTERMEDIATE_REGION_ID').display()

# COMMAND ----------


