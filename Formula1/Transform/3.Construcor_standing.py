# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC
# MAGIC %run "../Includes/common_function"

# COMMAND ----------

race_result_df=spark.read.parquet(f"{presenattion_path}/race_result")

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, count
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# Group by the required columns and aggregate with sum and count
Construcor_df = race_result_df.groupBy( 'race_year', 'team').agg(
    sum("points").alias("total_points"),
    count(when(col("position_Order") == 1, True)).alias("win")
)

# Window specification for ranking
Construcor_df_par = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("win"))

# Add rank column using rank() over the window
final_df = Construcor_df.withColumn("rank", rank().over(Construcor_df_par))



# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{presenattion_path}/Construcor_df")

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_presenation.Construcor_df")

# COMMAND ----------

