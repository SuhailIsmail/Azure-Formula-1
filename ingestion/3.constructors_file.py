# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors file
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

# Using DDL style for Defining Schema
constructors_schema = 'constructorId INT,\
                        constructorRef STRING,\
                        name STRING,\
                        nationality STRING,\
                        url STRING '

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2 - Drop unwanted columns, Rename columns and add ingestion.

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

constructor_final_rename = constructor_df.withColumnRenamed('constructorId','constructor_id')\
                                    .withColumnRenamed('constructorRef','constructor_ref')\
                                    .drop(col('url'))\
                                    .withColumn('data_source',lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final = add_ingestion_date(constructor_final_rename)

# COMMAND ----------

# MAGIC  %md
# MAGIC # Write the output to processed container in parquet format
# MAGIC

# COMMAND ----------

constructor_final.write.mode('overwrite').format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")