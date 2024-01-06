# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DoubleType,
)

orders_schema = StructType(
    [
        StructField("order_id", IntegerType(), True),
        StructField("ORDER_Timestamp", TimestampType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("ORDER_STATUS", StringType(), True),
        StructField("store_name", StringType(), True),
    ]
)
orderItems_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), True),
        StructField("PRODUCT_ID", IntegerType(), True),
        StructField("UNIT_PRICE", DoubleType(), True),
        StructField("QUANTITY", IntegerType(), True),
    ]
)
products_schema = StructType(
    [
        StructField("PRODUCT_ID", IntegerType(), True),
        StructField("PRODUCT_NAME", StringType(), True),
        StructField("UNIT_PRICE", DoubleType(), True),
    ]
)
customers_schema = StructType(
    [
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("FULL_NAME", StringType(), True),
        StructField("EMAIL_ADDRESS", StringType(), True),
    ]
)

# ORDER_DETAILS = spark.read.options(schema=orders_schema).parquet("dbfs:/FileStore/tables/silver/orders_parquet" )
orders = spark.read.options(schema=orders_schema).parquet(
    "dbfs:/FileStore/tables/silver/orders_parquet"
)
orderItems = spark.read.options(schema=orderItems_schema).parquet(
    "dbfs:/FileStore/tables/silver/orderItems"
)
products = spark.read.options(schema=products_schema).parquet(
    "dbfs:/FileStore/tables/silver/products"
)
customers = spark.read.options(schema=customers_schema).parquet(
    "dbfs:/FileStore/tables/silver/customers"
)

# COMMAND ----------

display(orders)
display(orderItems)

# COMMAND ----------

from pyspark.sql.functions import col,date_format

orders= orders.withColumn('order_date', date_format('ORDER_Timestamp', 'yyyy-MM-dd')).drop(
   'ORDER_Timestamp' 
)

# COMMAND ----------

orders.display()

# COMMAND ----------

orderDetails = orders.join(orderItems,orders['order_id'] == orderItems['order_id'],'left').select(orders['order_id'],col('CUSTOMER_ID'),col('store_name'),col('order_date'),col('UNIT_PRICE'))

orderDetails = orderDetails.groupBy('order_id','order_date','customer_id','store_name').sum('UNIT_PRICE')

display(orderDetails)

# COMMAND ----------

from pyspark.sql.functions import round,desc, asc

orderDetails= orderDetails.select('order_id','order_date','customer_id','store_name',round(col('sum(UNIT_PRICE)'),2).alias('Total_sales'))

# COMMAND ----------

orderDetails.write.parquet(mode='overwrite',path='dbfs:/FileStore/tables/Gold/orderDetails')
orderDetails.display()

# COMMAND ----------

monthlySales = orders.withColumn(
    "order_Month", date_format("order_date", "yyyy-MM")
).drop("ORDER_Timestamp")

monthlySales = monthlySales.join(
    orderItems, orders["order_id"] == orderItems["order_id"], "left"
).select(col("order_Month"), col("UNIT_PRICE"))
monthlySales.printSchema()

monthlySales = monthlySales.groupBy("order_Month").sum("UNIT_PRICE")
monthlySales= monthlySales.select('order_Month',round(col('sum(UNIT_PRICE)'),2).alias('Total_sales')).sort(desc('order_month'))
monthlySales.display()
monthlySales.write.parquet(mode='overwrite',path='dbfs:/FileStore/tables/Gold/monthlySales')

# COMMAND ----------

StoreMonthlySales = orders.withColumn(
    "order_Month", date_format("order_date", "yyyy-MM")
).drop("ORDER_Timestamp")

StoreMonthlySales = StoreMonthlySales.join(
    orderItems, orders["order_id"] == orderItems["order_id"], "left"
).select(col("order_Month"),col('store_name'), col("UNIT_PRICE"))
StoreMonthlySales.printSchema()

StoreMonthlySales = StoreMonthlySales.groupBy("order_Month", col('store_name')).sum("UNIT_PRICE")
StoreMonthlySales= StoreMonthlySales.select('order_Month',col('store_name'),round(col('sum(UNIT_PRICE)'),2).alias('Total_sales')).sort(desc('order_month'))
StoreMonthlySales.display()
StoreMonthlySales.write.parquet(mode='overwrite',path='dbfs:/FileStore/tables/Gold/StoreMonthlySales')

# COMMAND ----------


