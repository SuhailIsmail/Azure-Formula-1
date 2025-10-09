# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying json files
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1 - Read all JSON files in a Folder using spark dataframe

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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), False),
                                      StructField("number", IntegerType(), False),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine", True)\
    .json(f'{raw_folder}/{v_file_date}/qualifying')

# COMMAND ----------

# MAGIC %md
# MAGIC # Step - 2 
# MAGIC * Rename columns and add new columns
# MAGIC * Rename qualifyingId, driverId, constructorId and raceId
# MAGIC * Add ingestion_date with current timestamp
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("constructorId", "constructor_id")\
                        .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

qualify_final = add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC  %md
# MAGIC # Step -4 
# MAGIC * Write the output to processed container in parquet format
# MAGIC

# COMMAND ----------

# overwrite_partition(qualify_final, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id  "
merge_delta(qualify_final,'f1_processed','qualifying',processed_folder, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying