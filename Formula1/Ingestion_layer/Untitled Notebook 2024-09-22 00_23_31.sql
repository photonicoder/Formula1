-- Databricks notebook source
-- MAGIC %python
-- MAGIC import http.client
-- MAGIC
-- MAGIC conn = http.client.HTTPSConnection("linkedin-data-api.p.rapidapi.com")
-- MAGIC
-- MAGIC headers = {
-- MAGIC     'x-rapidapi-key': "5b9209c7d0mshac110a757d2a881p1cf79fjsn03828a4ca4d5",
-- MAGIC     'x-rapidapi-host': "linkedin-data-api.p.rapidapi.com"
-- MAGIC }
-- MAGIC
-- MAGIC conn.request("GET", "/data-connection-count?username=adamselipsky", headers=headers)
-- MAGIC
-- MAGIC res = conn.getresponse()
-- MAGIC data = res.read()
-- MAGIC
-- MAGIC print(data.decode("utf-8"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC data_json = json.loads(data.decode("utf-8"))
-- MAGIC print(json.dumps(data_json, indent=4))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC
-- MAGIC # Assuming you have a variable named 'data' containing the JSON string
-- MAGIC data_json = json.loads(data.decode("utf-8"))
-- MAGIC
-- MAGIC # Assuming you have a SparkSession named 'spark'
-- MAGIC df = spark.read.json(spark.sparkContext.parallelize([data_json]))