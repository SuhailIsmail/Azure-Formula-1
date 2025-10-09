# Databricks notebook source
v_result = dbutils.notebook.run("1.circuit_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.race_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.constructors_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.drivers_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.results_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.pit_stops_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.lap_times_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.qualifying_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc