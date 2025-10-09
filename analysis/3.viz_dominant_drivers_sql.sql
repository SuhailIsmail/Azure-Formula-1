-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT  
      drivers_name,
      COUNT(*) AS total_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points,
      RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
  GROUP BY drivers_name
  HAVING COUNT(*) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

SELECT  
      race_year,
      drivers_name,
      COUNT(*) AS total_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE drivers_name IN (SELECT drivers_name FROM v_dominant_drivers WHERE driver_rank <= 10)
  GROUP BY race_year,drivers_name
  ORDER BY race_year,avg_points DESC

-- COMMAND ----------

SELECT  
      race_year,
      drivers_name,
      COUNT(*) AS total_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE drivers_name IN (SELECT drivers_name FROM v_dominant_drivers WHERE driver_rank <= 10)
  GROUP BY race_year,drivers_name
  ORDER BY race_year,avg_points DESC

-- COMMAND ----------

SELECT  
      race_year,
      drivers_name,
      COUNT(*) AS total_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE drivers_name IN (SELECT drivers_name FROM v_dominant_drivers WHERE driver_rank <= 10)
  GROUP BY race_year,drivers_name
  ORDER BY race_year,avg_points DESC