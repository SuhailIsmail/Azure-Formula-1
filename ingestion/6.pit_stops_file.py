# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1 - Read JSON File using spark dataframe

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiLine", True) \
    .json(f'{raw_folder}/{v_file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC # Step - 2 
# MAGIC * Rename driverId and raceId
# MAGIC * Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id")\
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

pit_final = add_ingestion_date(pit_final_df)

# COMMAND ----------

# MAGIC  %md
# MAGIC # Step -4 
# MAGIC * Write the output to processed container in parquet format
# MAGIC

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta(pit_final,'f1_processed','pit_stops',processed_folder, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops