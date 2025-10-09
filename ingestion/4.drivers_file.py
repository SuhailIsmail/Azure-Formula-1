# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers file
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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField('forename', StringType(), True),
                                  StructField('surname', StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read \
    .option('header',True) \
    .schema(drivers_schema) \
    .json(f'{raw_folder}/{v_file_date}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2 - Rename columns and add new columns
# MAGIC * driverId renamed to driver_id  
# MAGIC * driverRef renamed to driver_ref  
# MAGIC * ingestion date added
# MAGIC * name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

# COMMAND ----------


drivers_with_columns = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                    .drop(col("url"))\
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC  %md
# MAGIC # Step 3 - Drop the unwanted columns

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_with_columns)

# COMMAND ----------

# MAGIC  %md
# MAGIC # Step -4 Write the output to processed container in parquet format
# MAGIC

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")