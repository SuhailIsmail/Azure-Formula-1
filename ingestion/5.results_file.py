# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results file
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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", IntegerType(), True)])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f'{raw_folder}/{v_file_date}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2 - Rename columns, add new columns and Drop the unwanted columns.

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

results_final_rename = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .drop(col('statusId'))\
                                    .withColumn('data_source', lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

results_final = add_ingestion_date(results_final_rename)

# COMMAND ----------

#Removing Duplicates
results_final_deduped = results_final.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC  %md
# MAGIC # Step -4 Write the output to processed container in parquet format
# MAGIC

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta(results_final_deduped,'f1_processed','results',processed_folder, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc