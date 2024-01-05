# Databricks notebook source
from pyspark.sql.functions import *

orderItems_path = 'dbfs:/FileStore/tables/Bronze/order_items.csv'

orders_path= 'dbfs:/FileStore/tables/Bronze/orders.csv'

products_path ='dbfs:/FileStore/tables/Bronze/products.csv'

stores_path='dbfs:/FileStore/tables/Bronze/stores.csv'

customers_path='dbfs:/FileStore/tables/Bronze/customers.csv'

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

display(orders)
display(products)
display(orderItems)
display(customers)
display(stores)


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

orders = orders.withColumn("ORDER_Timestamp", to_timestamp(col("ORDER_DATETIME"))).drop(
    col("ORDER_DATETIME")
)

# COMMAND ----------

orders = orders.join(stores, orders("STORE_ID") == stores("STORE_ID"), "inner")

# COMMAND ----------

orders=orders.select(
    orders("id"), orders("order_timestamp"), orders("customer_id"), stores("STORE_NAME")
)
