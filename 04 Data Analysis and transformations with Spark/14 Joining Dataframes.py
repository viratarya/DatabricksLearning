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

regions_path = 'dbfs:/FileStore/tables/country_regions.csv'



# COMMAND ----------

# df=spark.read.options(header=True).options(inferSchema=True).csv(regions_path)
# df.schema

# COMMAND ----------

from pyspark.sql.types import IntegerType,StructType,StructField,StringType

region_schema = StructType([StructField('ID', IntegerType(), False),
            StructField('NAME', StringType(), False)])
regions = spark.read.csv(path=regions_path,header=True, schema=region_schema)

# COMMAND ----------

countries.display()
regions.display()

# COMMAND ----------

countries.join(regions,countries['region_id']==regions['id'],'inner').display()

# COMMAND ----------

countries.join(regions,countries['region_id']==regions['id'],'right').display()

# COMMAND ----------

countries.join(regions,countries['region_id']==regions['id'],'right').select(countries['name'],regions['name'],countries['population']).display()

# COMMAND ----------

from pyspark.sql.functions import col

population_region_vise = countries.join(
    regions, countries["region_id"] == regions["id"], "right"
)
population_region_vise1 = (
    population_region_vise.select(
        countries["name"].alias("Country_name"),
        regions["name"].alias("Region_name"),
        countries["population"],
    )
    .sort(countries["population"].desc())
    .display()
)

# .withColumnRenamed(col(countries['name']), 'Country_name')\
# .withColumnRenamed(col(regions['name']),'Region_name') \

# COMMAND ----------

# from pyspark.sql.functions import col

# population_region_vise = countries.join(
#     regions, countries["region_id"] == regions["id"], "right"
# )\
# .select(
#         countries["name"],
#         regions["name"],
#         countries["population"],
#     )\
# .withColumnRenamed(col(countries["name"]),"countries_name")\
# .withColumnRenamed(regions["name"],"regions_name")\
# .sort(countries["population"].desc())\
# .display()


# COMMAND ----------


