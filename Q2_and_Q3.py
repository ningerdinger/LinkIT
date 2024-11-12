# Databricks notebook source
# MAGIC %md
# MAGIC ASSIGNMENT 2

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, hour, minute, second, sum
import json
from datetime import datetime

# Config dictionary with Spark tables
config = {
    "df_customer": spark.table("hive_metastore.default.customer_data"),
    "df_cc": spark.table("hive_metastore.default.creditcard_transaction"),
    "df_prod": spark.table("hive_metastore.default.product_transaction"),
    "Q2A": {
        "top_n": 100,
        "select_col_2A": ["CUSTOMER_NUMBER", "TRANSACTION_ID", "TRANSACTION_VALUE", "TRANSACTION_DATE_TIME"],
        "ORDER_BY": "TRANSACTION_VALUE"
    },
    "Q2B": {
        "col1": "TRANSACTION_ID",
        "col2": "CUSTOMER_NUMBER",
        "select_col_2B": ["CUSTOMER_NUMBER", "TRANSACTION_ID", "TRANSACTION_VALUE", "TRANSACTION_DATE_TIME"],
    },
    "Q3A": {
        "url": "/Workspace/Users/ningweizhou1991@gmail.com/json_outputs",
        "top_n": 10
    }
}


class process_data:
    def __init__(self, config: dict):
        self.df_customer = config["df_customer"]
        self.df_cc = config["df_cc"]
        self.df_prod = config["df_prod"]

        # Question 2a
        self.select_column_2A = config["Q2A"]["select_col_2A"]
        self.transaction_val = config["Q2A"]["ORDER_BY"]
        self.top_n = config["Q2A"]["top_n"]
        self.df_2A = self.generate_df_top_n_cc(self.select_column_2A, self.transaction_val, self.top_n, self.df_cc)

        # Question 2b
        self.col1 = config["Q2B"]["col1"]
        self.col2 = config["Q2B"]["col2"]
        self.select_column_2B = config["Q2B"]["select_col_2B"]

        self.combined_1 = self.generate_join_df(self.df_cc, self.df_prod, self.col1)
        self.combined_2 = self.generate_join_df(self.combined_1, self.df_customer, self.col2)
        self.df_2B_unfiltered = self.combined_2.select("TRANSACTION_DATE_TIME", "ITEM_VALUE", "ITEM_QUANTITY", "`CREDITCARD.PROVIDER`")

        #filtering 2b
        self.period = 12
        self.df_2B_ungrouped = self.df_2B_unfiltered.filter(hour(col("TRANSACTION_DATE_TIME")) < self.period).orderBy("TRANSACTION_DATE_TIME", ascending=False)

        #grouping together by creditcard provider
        self.df_2B = self.df_2B_ungrouped.groupBy("`CREDITCARD.PROVIDER`").agg( \
            sum("ITEM_QUANTITY").alias("Total_quantity"), \
            sum("ITEM_VALUE").alias("Total_value") \
            )
        
        # Question 3
        self.q3_top_n = config["Q3A"]["top_n"]
        self.url = config["Q3A"]["url"]
        self.df_aggregated = self.combined_2.orderBy(self.col1, ascending=False).limit(self.q3_top_n)
        self.write_to_json(self.df_aggregated, self.url)
        

    def generate_df_top_n_cc(self, column: list, orderby: str, top_n: int, df: DataFrame) -> DataFrame:
        df_top_customers = df \
            .select(*column) \
            .orderBy(orderby, ascending=False) \
            .limit(top_n)
        return df_top_customers

    def generate_join_df(self, df1: DataFrame, df2: DataFrame, join_col: str) -> DataFrame:
        df_combined = df1.join(df2, join_col)
        return df_combined
    
    def datetime_converter(self, obj: datetime): 
        if isinstance(obj, datetime): 
            return obj.isoformat()

    def write_to_json(self, df: DataFrame, output_path: str):
        for i in range(df.count()):
            row = df.collect()[i].asDict()
            json_data = json.dumps(row, default=datetime_converter)
            with open(f"{output_path}/json_file_{i}.json", 'w') as file: 
                json.dump(json_data, file)

# Example usage
test = process_data(config=config)




# COMMAND ----------

test.df_2A.display()

# COMMAND ----------

test.df_2B.display()

# COMMAND ----------

# select_col = ["CUSTOMER_NUMBER", "TRANSACTION_ID", "TRANSACTION_VALUE", "TRANSACTION_DATE_TIME"]
# order_var = "TRANSACTION_VALUE"
# top_n = 100

# def generate_df_top_n(column: list, order_var: str, top_n: int, df: DataFrame): 
#     df_top_customers = df \
#         .select(*column) \
#         .orderBy(order_var, ascending=False) \
#         .limit(top_n)
#     return df_top_customers

# df_top_customers = generate_df_top_n(select_col, order_var, top_n, df_cc)
# df_top_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Question_2B

# COMMAND ----------

# df_combined = df_cc.join(df_prod, 'TRANSACTION_ID').join(df_customer, "CUSTOMER_NUMBER")
# df_new = df_combined.select("TRANSACTION_DATE_TIME", "ITEM_VALUE", "ITEM_QUANTITY", "`CREDITCARD.PROVIDER`")


# COMMAND ----------

# df_noon_sales = df_new.filter(hour(col("TRANSACTION_DATE_TIME")) < 12).orderBy("TRANSACTION_DATE_TIME", ascending=False)

# #showing that it works properly by splitting hours, min and seconds to showcase filtering
# df_example = df_noon_sales.withColumn("hour", hour(col("TRANSACTION_DATE_TIME"))) \
#         .withColumn("minute", minute(col("TRANSACTION_DATE_TIME"))) \
#         .withColumn("second", second(col("TRANSACTION_DATE_TIME"))) \
#         .orderBy("hour", ascending=False)
# df_example.display()


# COMMAND ----------

# result_q2 = df_noon_sales.groupBy("`CREDITCARD.PROVIDER`").agg( \
#     sum("ITEM_QUANTITY").alias("Total_quantity"), \
#     sum("ITEM_VALUE").alias("Total_value") \
#         ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ASSIGNMENT 3

# COMMAND ----------

# df_aggregated = df_combined.orderBy("TRANSACTION_VALUE", ascending=False).limit(100)
# df_aggregated.display()

# COMMAND ----------

# type(test)

# COMMAND ----------

# from datetime import datetime

# output = "/Workspace/Users/ningweizhou1991@gmail.com/json_outputs"

# def datetime_converter(o): 
#     if isinstance(o, datetime): 
#         return o.isoformat()

# for i in range(df_aggregated.count()):
#     test = df_aggregated.collect()[i].asDict()
#     json_data = json.dumps(test, default=datetime_converter)
#     with open(f"{output_path}/json_file_{i}.json", 'w') as file: 
#         json.dump(json_data, file)

# COMMAND ----------

# df_prod.columns

# COMMAND ----------

# df_customer.columns

# COMMAND ----------

# df_cc.columns

# COMMAND ----------

# df_combined.columns