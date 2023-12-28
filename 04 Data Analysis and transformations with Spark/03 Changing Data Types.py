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

# COMMAND ----------

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

# COMMAND ----------

Countries = spark.read.options(header= True).options(schema= 'Countries_schema').csv(path= countries_path)

# COMMAND ----------

Countries.dtypes

# COMMAND ----------

Countries_dt = spark.read.csv(path= countries_path, header=True)

# COMMAND ----------

Countries_dt.dtypes

# COMMAND ----------

Countries_dt.select(Countries_dt['population'].cast(IntegerType())).dtypes

# COMMAND ----------

Countries.select(Countries.POPULATION.cast(StringType())).dtypes

# COMMAND ----------

Countries.select(Countries['POPULATION']/10000000).withColumnRenamed('(POPULATION / 10000000)','population_m').display()

# COMMAND ----------

Countries_2 = Countries.select(Countries['POPULATION']/10000000).withColumnRenamed('(POPULATION / 10000000)','population_m')

# COMMAND ----------

Countries_2.display()

# COMMAND ----------

from pyspark.sql.functions import round 

Countries_2.select(round(Countries_2['population_m'],1)).display()

# COMMAND ----------

Countries.withColumn('population_m',round(Countries['population']/1000000,1)).display()

# COMMAND ----------


