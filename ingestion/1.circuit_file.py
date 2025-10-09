# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Circuit file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Read CSV File using spark dataframe

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType

# COMMAND ----------

circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)])

# COMMAND ----------

circuit_df = spark.read.option('header',True).schema(circuit_schema).csv(f'{raw_folder}/{v_file_date}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2 - Select only the required columns

# COMMAND ----------

circuit_df.columns

# COMMAND ----------

from pyspark.sql.functions import col
circuit_selected = circuit_df.select(col('circuitId'),
                                    col('circuitRef'),
                                    col('name'),
                                    col('location'),
                                    col('country'),
                                    col('lat'),
                                    col('lng'),
                                    col('alt'))

# COMMAND ----------

# MAGIC  %md
# MAGIC # Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuit_renamed = circuit_selected.withColumnRenamed("circuitId", "circuit_id") \
                                    .withColumnRenamed("circuitRef", "circuit_ref") \
                                    .withColumnRenamed("lat", "latitude") \
                                    .withColumnRenamed("lng", "longitude") \
                                    .withColumnRenamed("alt", "altitude") \
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC  %md 
# MAGIC # Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuit_final = add_ingestion_date(circuit_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuit_final.write.mode('overwrite').format("delta").saveAsTable('f1_processed.circuits')

# COMMAND ----------

dbutils.notebook.exit("Success")