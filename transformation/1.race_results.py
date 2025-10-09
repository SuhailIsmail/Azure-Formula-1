# Databricks notebook source
dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading all the data as required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f'{processed_folder}/circuits')\
    .withColumnRenamed('name','circuit_name')\
    .withColumnRenamed('location','circuit_location')

# COMMAND ----------

races_df = spark.read.format("delta").load(f'{processed_folder}/races')\
            .withColumnRenamed('name','race_name')\
            .withColumnRenamed('race_timestamp','race_date')

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f'{processed_folder}/drivers')\
    .withColumnRenamed('name','driver_name')\
    .withColumnRenamed('number','driver_number')\
    .withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

constructor_df = spark.read.format("delta").load(f'{processed_folder}/constructors')\
    .withColumnRenamed('name','team')\
    .withColumnRenamed('nationality','team_nationality')

# COMMAND ----------

result_df = spark.read.format("delta").load(f'{processed_folder}/results')\
            .filter(f"file_date = '{v_file_date}'")\
            .withColumnRenamed('time','race_time')\
            .withColumnRenamed('race_id','result_race_id')\
            .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id,'inner')\
    .select(races_df.race_id,races_df.race_year, races_df.race_name,races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC # Join results to all other dataframes

# COMMAND ----------

race_result_df = result_df.join(race_circuits_df, result_df.result_race_id == race_circuits_df.race_id)\
                .join(drivers_df, drivers_df.driver_id == result_df.driver_id)\
                .join(constructor_df, constructor_df.constructor_id == result_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_result_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
                                .withColumn("created_date", current_timestamp())\
                                .withColumnRenamed("result_file_date", "file_date")                

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"
merge_delta(final_df,'f1_presentation','race_results',presentation_folder, merge_condition,'race_id')

# COMMAND ----------

# %sql
# select race_id,count(*)
# from f1_presentation.race_results
# group by race_id
# order by race_id desc