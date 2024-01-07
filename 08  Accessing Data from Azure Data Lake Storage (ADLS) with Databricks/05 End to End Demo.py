# Databricks notebook source
application_id = dbutils.secrets.get(scope="databrics-secrets-va0209",key="applicationId")
tenant_id = dbutils.secrets.get(scope="databrics-secrets-va0209",key="tenantId")
secret = dbutils.secrets.get(scope="databrics-secrets-va0209",key="secretsValue")


container_name ="silver"
account_name = "datalakeva0209"
mount_point ='/mnt/silver'

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType
countries_path ='/mnt/bronze/countries.csv'
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

countries=spark.read.csv(path=countries_path, header=True, schema=Countries_schema)
display(countries)

# COMMAND ----------

countries=countries.drop('SUB_REGION_ID','INTERMEDIATE_REGION_ID','ORGANIZATION_REGION_ID')

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.write.parquet('/mnt/silver/countries')

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType

regions_path = '/mnt/bronze/country_regions.csv'

Region_Schema = StructType(
    [StructField("ID", StringType(), False), StructField("NAME", StringType(), False)]
)
regions = spark.read.csv(path=regions_path,header=True,schema = Region_Schema)

# COMMAND ----------

regions.display()

# COMMAND ----------

regions=regions.withColumnRenamed("NAME","REGION_NAME")

# COMMAND ----------

regions.display()

# COMMAND ----------

regions.write.parquet('/mnt/silver/regions')

# COMMAND ----------

countries = spark.read.parquet('/mnt/silver/countries')
regions = spark.read.parquet('/mnt/silver/regions')

# COMMAND ----------

countries.display()
regions.display()



# COMMAND ----------

country_data=countries.join(regions,countries['region_id']==regions['Id'],'left').drop('Id','region_id','country_code','iso_alpha_2')

# COMMAND ----------

country_data.display()

# COMMAND ----------

country_data.write.parquet('/mnt/gold/countries_data', mode='overwrite')

# COMMAND ----------


