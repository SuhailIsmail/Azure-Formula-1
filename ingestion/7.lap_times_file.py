# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap times file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1 - Read all CSV  files in a Folder using spark dataframe

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

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f'{raw_folder}/{v_file_date}/lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC # Step - 2 
# MAGIC * Rename columns and add new columns
# MAGIC * Rename driverId and raceId
# MAGIC * Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                            .withColumnRenamed("raceId", "race_id")\
                            .withColumn("data_source", lit(v_data_source))\
                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

lap_final = add_ingestion_date(lap_final_df)

# COMMAND ----------

# MAGIC  %md
# MAGIC # Step -4 
# MAGIC * Write the output to processed container in parquet format
# MAGIC

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta(lap_final,'f1_processed','lap_times',processed_folder, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times