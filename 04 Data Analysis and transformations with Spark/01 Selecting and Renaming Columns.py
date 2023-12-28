# Databricks notebook source
countries_path = 'dbfs:/FileStore/tables/countries.csv'

df = spark.read.options(header=True).options(inferSchema= True).csv('dbfs:/FileStore/tables/countries.csv')

# COMMAND ----------

df.schema

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

display(Countries)

# COMMAND ----------

df= Countries.select('name','capital','population').display()

# COMMAND ----------

Countries.select(Countries['name'],Countries['capital'],Countries['population']).display()

# COMMAND ----------

Countries.select(Countries.NAME,Countries.CAPITAL,Countries.REGION_ID).display()

# COMMAND ----------

from pyspark.sql.functions import col
df1= Countries.select(col('name'),col('capital'),col('population')).display()

# COMMAND ----------

Countries.select(Countries['name'],Countries['capital'],Countries['population']) \
.withColumnRenamed('name','Countries_name') \
.withColumnRenamed('capital','Capital_name').display()

# COMMAND ----------

Countries.select(Countries['name'].alias('Countries_name'),Countries['capital'].alias('Capital_name'),Countries['population']).display()

# COMMAND ----------

Countries.select(col('name').alias('Countries_name')).display()

# COMMAND ----------

Countries.select(Countries.NAME,Countries.CAPITAL,Countries.POPULATION).alias('va').display()

# COMMAND ----------

CountriesRegion_path='dbfs:/FileStore/tables/country_regions.csv'

# COMMAND ----------

CountriesRegion = spark.read.options(header='True').csv(path=CountriesRegion_path)

# COMMAND ----------

CountriesRegion.schema

# COMMAND ----------

from pyspark.sql.types import StringType,StructField,StructType,IntegerType

CountriesRegion_Schema= StructType([StructField('ID', StringType(), False),
            StructField('NAME', StringType(), False)])
            

# COMMAND ----------

CountriesRegion = spark.read.options(header='True').options(schema= CountriesRegion_Schema).csv(path=CountriesRegion_path)

# COMMAND ----------

CountriesRegion = spark.read.options(header='True').csv(path=CountriesRegion_path, schema= CountriesRegion_Schema)

# COMMAND ----------

from pyspark.sql.functions import current_date

Countries.withColumn('Current_date',current_date()).display()

# COMMAND ----------

CountriesRegion.display()

# COMMAND ----------

from pyspark.sql.functions import lit

Countries.withColumn('updated_by',lit('VA')).display()

# COMMAND ----------

Countries.withColumn('population_m',Countries['population']/10000000).display()

# COMMAND ----------


