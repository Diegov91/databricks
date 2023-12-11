# Databricks notebook source
customers=spark.read.csv(path="dbfs:/FileStore/shared_uploads/marcodiaz32611@gmail.com/CustomerOrders/customers.csv", inferSchema=True,header=True)
orders=spark.read.csv(path="dbfs:/FileStore/shared_uploads/marcodiaz32611@gmail.com/CustomerOrders/orders.csv", inferSchema=True,header=True)
order_items=spark.read.csv(path="dbfs:/FileStore/shared_uploads/marcodiaz32611@gmail.com/CustomerOrders/order_items.csv", inferSchema=True,header=True)


# COMMAND ----------

display(customers)

# COMMAND ----------

display(orders)

# COMMAND ----------

cust_order_join=customers.join(orders,customers.CUSTOMER_ID==orders.CUSTOMER_ID,"left")
display(cust_order_join)

# COMMAND ----------


