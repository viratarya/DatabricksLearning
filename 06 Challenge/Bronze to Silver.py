# Databricks notebook source
from pyspark.sql.functions import *

orderItems_path = 'dbfs:/FileStore/tables/bronze/order_items.csv'

orders_path= 'dbfs:/FileStore/tables/bronze/orders.csv'

products_path ='dbfs:/FileStore/tables/bronze/products.csv'

stores_path='dbfs:/FileStore/tables/bronze/stores.csv'

customers_path='dbfs:/FileStore/tables/bronze/customers.csv'

# orders = spark.read.csv(inferSchema=True,header=True, path=orders_path)
# products = spark.read.csv(inferSchema=True,header=True, path=products_path)

# orderItems = spark.read.csv(inferSchema=True,header=True, path=orderItems_path)
# products = spark.read.csv(inferSchema=True,header=True, path=products_path)

# stores = spark.read.csv(inferSchema=True,header=True, path=stores_path)

# customers = spark.read.csv(inferSchema=True,header=True, path=customers_path)



# COMMAND ----------

# orders.schema 

# print('--------')
#products.schema

# orderItems.schema
# products.schema

# stores.schema
# customers.schema



# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)


orders_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("ORDER_DATETIME", StringType(), False),
        StructField("CUSTOMER_ID", IntegerType(), False),
        StructField("ORDER_STATUS", StringType(), False),
        StructField("STORE_ID", IntegerType(), True),
    ]
)
products_schema = StructType(
    [
        StructField("PRODUCT_ID", IntegerType(), False),
        StructField("PRODUCT_NAME", StringType(), False),
        StructField("UNIT_PRICE", DoubleType(), False),
    ]
)

orderItems_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("LINE_ITEM_ID", IntegerType(), True),
        StructField("PRODUCT_ID", IntegerType(), False),
        StructField("UNIT_PRICE", DoubleType(), False),
        StructField("QUANTITY", IntegerType(), False),
    ]
)
customers_schema = StructType(
    [
        StructField("CUSTOMER_ID", IntegerType(), False),
        StructField("FULL_NAME", StringType(), False),
        StructField("EMAIL_ADDRESS", StringType(), False),
    ]
)

# COMMAND ----------

orders = spark.read.csv(schema=orders_schema,header=True, path=orders_path)
products = spark.read.csv(schema=products_schema,header=True, path=products_path)

orderItems = spark.read.csv(schema=orderItems_schema,header=True, path=orderItems_path)


stores = spark.read.csv(inferSchema=True,header=True, path=stores_path)

customers = spark.read.csv(schema=customers_schema,header=True, path=customers_path)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

orders = orders.select((to_timestamp(col("ORDER_DATETIME"),'dd-MMM-yy kk.mm.ss.SS')).alias('ORDER_Timestamp'),
                       'ORDER_ID',
                       'CUSTOMER_ID',
                       'ORDER_STATUS',
                       'STORE_ID').drop(
    col("ORDER_DATETIME")
).filter(col('ORDER_STATUS') == 'COMPLETE')
display(orders)

# COMMAND ----------

orders = orders.join(stores, orders["STORE_ID"] == stores["STORE_ID"], "left").select(col('order_id'),col('ORDER_Timestamp'),col('CUSTOMER_ID'),col('ORDER_STATUS'),col('store_name'))

# COMMAND ----------

display(orders)

# COMMAND ----------

orders.write.parquet('/FileStore/tables/silver/orders_parquet',mode='overwrite')

# COMMAND ----------

display(orderItems)

# COMMAND ----------

orderItems= orderItems.drop('LINE_ITEM_ID')

# COMMAND ----------

display(orderItems)

# COMMAND ----------

orderItems.write.parquet('/FileStore/tables/silver/orderItems',mode='overwrite')

# COMMAND ----------

display(products)

# COMMAND ----------

products.write.parquet('/FileStore/tables/silver/products',mode='overwrite')

# COMMAND ----------

display(customers)

# COMMAND ----------

customers.printSchema()

# COMMAND ----------

customers.write.parquet('/FileStore/tables/silver/customers',mode='overwrite')

# COMMAND ----------

orders.schema

# COMMAND ----------

products.schema

# COMMAND ----------

orderItems.schema

# COMMAND ----------

customers.schema

# COMMAND ----------

orderItems_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), True),
        StructField("PRODUCT_ID", IntegerType(), True),
        StructField("UNIT_PRICE", DoubleType(), True),
        StructField("QUANTITY", IntegerType(), True),
    ]
)

orderItems = spark.read.options(schema= orderItems_schema).parquet('/FileStore/tables/silver/orderItems')

display(orderItems)

# COMMAND ----------


