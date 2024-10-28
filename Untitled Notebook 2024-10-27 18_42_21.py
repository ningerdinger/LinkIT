# Databricks notebook source
# MAGIC %md
# MAGIC ASSIGNMENT 2

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, hour, minute, second, sum
import json

df_customer = spark.table("hive_metastore.default.customer_data")
df_cc = spark.table("hive_metastore.default.creditcard_transaction")
df_prod = spark.table("hive_metastore.default.product_transaction")

# COMMAND ----------

df_customer.display()

# COMMAND ----------

df_cc.display()

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_top_customers = df_cc \
    .select("CUSTOMER_NUMBER", "TRANSACTION_ID", "TRANSACTION_VALUE", "TRANSACTION_DATE_TIME") \
    .orderBy("TRANSACTION_VALUE", ascending=False) \
    .limit(100)


# COMMAND ----------

# MAGIC %md
# MAGIC Question_2B

# COMMAND ----------

df_top_customers.display()

# COMMAND ----------

df_combined = df_cc.join(df_prod, 'TRANSACTION_ID').join(df_customer, "CUSTOMER_NUMBER")
df_new = df_combined.select("TRANSACTION_DATE_TIME", "ITEM_VALUE", "ITEM_QUANTITY", "`CREDITCARD.PROVIDER`")


# COMMAND ----------

df_noon_sales = df_new.filter(hour(col("TRANSACTION_DATE_TIME")) < 12).orderBy("TRANSACTION_DATE_TIME", ascending=False)

#showing that it works properly by splitting hours, min and seconds to showcase filtering
df_example = df_noon_sales.withColumn("hour", hour(col("TRANSACTION_DATE_TIME"))) \
        .withColumn("minute", minute(col("TRANSACTION_DATE_TIME"))) \
        .withColumn("second", second(col("TRANSACTION_DATE_TIME"))) \
        .orderBy("hour", ascending=False)
df_example.display()


# COMMAND ----------

result_q2 = df_noon_sales.groupBy("`CREDITCARD.PROVIDER`").agg( \
    sum("ITEM_QUANTITY").alias("Total_quantity"), \
    sum("ITEM_VALUE").alias("Total_value") \
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ASSIGNMENT 3

# COMMAND ----------

df_aggregated = df_combined.orderBy("TRANSACTION_VALUE", ascending=False).limit(100)
df_aggregated.display()

# COMMAND ----------

type(test)

# COMMAND ----------

from datetime import datetime

output = "/Workspace/Users/ningweizhou1991@gmail.com/json_outputs"

def datetime_converter(o): 
    if isinstance(o, datetime): 
        return o.isoformat()

for i in range(df_aggregated.count()):
    test = df_aggregated.collect()[i].asDict()
    json_data = json.dumps(test, default=datetime_converter)
    with open(f"{output_path}/json_file_{i}.json", 'w') as file: 
        json.dump(json_data, file)

# COMMAND ----------

df_prod.columns

# COMMAND ----------

df_customer.columns

# COMMAND ----------

df_cc.columns

# COMMAND ----------

df_combined.columns
