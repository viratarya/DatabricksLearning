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

regions_path = "dbfs:/FileStore/tables/country_regions.csv"

regions_schema = StructType(
    [StructField("ID", IntegerType(), False), StructField("NAME", StringType(), False)]
)

regions = (
    spark.read.options(header=True)
    .options(schema=regions_schema)
    .csv(path=regions_path)
)

# COMMAND ----------

countries = countries.join(regions, countries['region_id']==regions['id'],'inner').select(countries['name'].alias('country_name'),
                                                                              regions['name'].alias('region_name'),countries['population'])

# COMMAND ----------

pivot_countries= countries.groupby('country_name').pivot('region_name').sum('population')


# COMMAND ----------

pivot_countries.display()

# COMMAND ----------

from pyspark.sql.functions import expr

pivot_countries.select(
    "country_name",
    expr(
        "stack(5,'Africa',Africa,'America',America,'Asia',Asia,'Europe',Europe,'Oceania',Oceania) as (region_name, population)"
    ),
).filter('population is not null')\
.display()

# COMMAND ----------


