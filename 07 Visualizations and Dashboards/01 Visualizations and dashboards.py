# Databricks notebook source
orderDetails = spark.read.parquet('dbfs:/FileStore/tables/Gold/orderDetails')

monthlySales = spark.read.parquet('dbfs:/FileStore/tables/Gold/monthlySales')

# COMMAND ----------

display(orderDetails)
display (monthlySales)

# COMMAND ----------

orderDetails.display()

# COMMAND ----------

monthlySales.display()

# COMMAND ----------

# MAGIC %md Some Text for my notebook

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales by Month

# COMMAND ----------

# MAGIC %md
# MAGIC # Monthly Sales by Stores

# COMMAND ----------

displayHTML("""<font size="6" color="red" face="sans-serif">SalesDashboard</font>""")
