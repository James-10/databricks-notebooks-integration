# Databricks notebook source
import requests
import json

response = requests.get("https://carapi.app/api/makes")

# COMMAND ----------

#Transform data into a dictionary
data = response.json()['data']
print(data)

# COMMAND ----------

df = spark.createDataFrame(data)
df.write.mode("overwrite").saveAsTable("default.car_makes")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM default.car_makes LIMIT 100
