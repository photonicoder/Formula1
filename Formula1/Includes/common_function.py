# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit
def add_ingestion_date(input_df, my_source):
    output_df=input_df.withColumn("ingestion_date",current_timestamp()).withColumn("data_source", lit(my_source))
    return output_df

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# def rearrange_partiton(input_df,parttion_column):
#     column_list=[]
#     for column_name in input_df.schema.names:
#         if column_name != parttion_column:
#             column_list.append(column_name)
#     column_list.append(parttion_column)
#     output_df=input_df.select(column_list)
#     return output_df

# COMMAND ----------

def rearrange_partition(input_df, partition_column):
    # Rearranging the columns so that partition_column is the last column
    column_list = [col for col in input_df.schema.names if col != partition_column]
    column_list.append(partition_column)
    
    # Select columns in the correct order
    output_df = input_df.select(column_list)
    return output_df

def overwrite_partition(input_df, db_name, table_name, partition_column):
    # Rearrange the DataFrame so that the partition column is last
    output_df = rearrange_partition(input_df, partition_column)
    
    # Enable dynamic partition overwrite mode
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # Check if the table already exists
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        # If the table exists, append the new data
        output_df.write.mode("append").insertInto(f"{db_name}.{table_name}")
    else:
        # If the table doesn't exist, create it and overwrite if necessary
        output_df.write.mode("overwrite") \
            .partitionBy(partition_column) \
            .format("delta") \
            .saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

# def overwrite_partition(input_df, db_name, table_name, partition_column):
#     output_df = rearrange_partiton(input_df, partition_column)
#     spark.conf.set("spark.sql.source.partitionOverwriteMode", "dynamic")
#     if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
#         output_df.write.mode("append").insertInto(f"{db_name}.{table_name}")
#     else:
#         output_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# def check_existing_file(results_df,id,table):
#   for listed in results_df.select(f"{id}").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists(f"f1_processed.{table}")):
#         spark.sql(f"DELETE FROM f1_processed.{table} WHERE race_Id = {listed.id}")