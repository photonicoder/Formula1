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

race_result_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, count
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# Group by the required columns and aggregate with sum and count
driver_standing_df = race_result_df.groupBy("driver_name", 'race_year', 'team', 'driver_nationality').agg(
    sum("points").alias("total_points"),
    count(when(col("position_Order") == 1, True)).alias("win")
)

# Window specification for ranking
driver_standing_par = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("win"))

# Add rank column using rank() over the window
final_df = driver_standing_df.withColumn("rank", rank().over(driver_standing_par))



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc
driver_standing_par=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("win"))
final_df=driver_standing_df.withColumn("rank",rank().over(driver_standing_par))
#final_df = driver_standing_df.withColumn("rank", rank().over(driver_standing_par))
#final_df=driver_standing_df.withColumn("rank",rank().over(driver_standing_par))

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{presenattion_path}/Driver_Standing")

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_presenation.Driver_Standing")

# COMMAND ----------

