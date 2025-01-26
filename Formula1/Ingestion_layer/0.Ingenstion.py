# Databricks notebook source
v_result = dbutils.notebook.run("Ingest circuits", 0, {"p_data_source": "Ergest API"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest Constructor", 0, {"p_data_source": "Ergest API"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("Ingest laps_times",0,{"p_data_source":"Ergest API"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("Ingest Driver ",0,{"p_data_source":"Ergest API"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("Ingest pit_stops",0,{"p_data_source":"Ergest API"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("Ingest Qualifying",0,{"p_data_source":"Ergest API"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("Ingest races",0,{"p_data_source":"Ergest API"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result=dbutils.notebook.run("Ingest Result ",0,{"p_data_source":"Ergest API"})

# COMMAND ----------

print(v_result)