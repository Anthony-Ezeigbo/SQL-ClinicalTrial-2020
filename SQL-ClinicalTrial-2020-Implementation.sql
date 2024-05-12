-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Data Preparation for clinical dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Assign variable name to each file name to make the code reuseable
-- MAGIC  
-- MAGIC clinicaltrial = "clinicaltrial_2020"
-- MAGIC pharma = "pharma"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  
-- MAGIC from pyspark.sql.functions import to_date, date_format, col
-- MAGIC  
-- MAGIC #Read the dataset into a dataframe and create a view/temporary table from the dataframe
-- MAGIC clinicaltrial_df = spark.read.options(delimiter="|", header = True).csv("/FileStore/tables/" + clinicaltrial + ".csv")
-- MAGIC  
-- MAGIC #Convert Completion column to date type
-- MAGIC clinicaltrial_df = clinicaltrial_df.withColumn('Completion', to_date('Completion', 'MMM yyyy'))
-- MAGIC  
-- MAGIC #create a view/temporary table from the dataframe
-- MAGIC clinicaltrial_df.createOrReplaceTempView("clinicaltrial")

-- COMMAND ----------

Describe clinicaltrial

-- COMMAND ----------

SELECT * FROM clinicaltrial

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 1 - Spark SQL

-- COMMAND ----------

select count (distinct(id)) as NumberOfDistinctStudies from clinicaltrial

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 2 - Spark SQL

-- COMMAND ----------

Select type, count(*) as count from clinicaltrial
Group by type
Order by count Desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 3 - Spark SQL

-- COMMAND ----------

Select Condition, COUNT(condition) As Count
From (
  Select 
    Explode(Split(conditions, ',')) As Condition
  From clinicaltrial
  Where conditions Is Not NULL
) 
Group by Condition
Order by Count Desc Limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Data Preparation for pharma dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  
-- MAGIC #Read the dataset into a dataframe and create a view/temporary table from the dataframe
-- MAGIC Pharmaceuticals_df = spark.read.options(delimiter= ",", header = True).csv("/FileStore/tables/" + pharma + ".csv") 
-- MAGIC  
-- MAGIC #create a view/temporary table from the dataframe
-- MAGIC Pharmaceuticals_df.createOrReplaceTempView("Pharmaceuticals")

-- COMMAND ----------

---View contents of the pharma table
select * from Pharmaceuticals

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 4 - Spark SQL

-- COMMAND ----------

Select sponsor, COUNT(id) As NumberOfTrials
From clinicaltrial
Where sponsor not in (
  Select parent_company
  From Pharmaceuticals
)
Group by sponsor
Order by NumberOfTrials Desc
Limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 5 - Spark SQL

-- COMMAND ----------

Select MonthName, NumberOfCompletedtrial from
(select
case
    when Date_format(Completion, 'MMM') = 'Jan' THEN 1
    when Date_format(Completion, 'MMM') = 'Feb' THEN 2 
    when Date_format(Completion, 'MMM') = 'Mar' THEN 3 
    when Date_format(Completion, 'MMM') = 'Apr' THEN 4 
    when Date_format(Completion, 'MMM') = 'May' THEN 5
    when Date_format(Completion, 'MMM') = 'Jun' THEN 6 
    when Date_format(Completion, 'MMM') = 'Jul' THEN 7 
    when Date_format(Completion, 'MMM') = 'Aug' THEN 8 
    when Date_format(Completion, 'MMM') = 'Sep' THEN 9 
    when Date_format(Completion, 'MMM') = 'Oct' THEN 10 
    when Date_format(Completion, 'MMM') = 'Nov' THEN 11 
    when Date_format(Completion, 'MMM') = 'Dec' THEN 12 
  END as month, 
  Date_format(Completion, 'MMM') as MonthName, Count(*) as NumberOfCompletedtrial
from (select Status, Completion from clinicaltrial
where Status = 'Completed' and Completion like('%2021%'))
Group By MonthName
order by  month  ASC)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##EXTRA FEATURE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Problem statement
-- MAGIC ##Analyse the number of clinical trials done by pharmaceutical companies per country

-- COMMAND ----------

---Count the number of parent sponsors by Country and sort in descending order
SELECT p.HQ_Country_of_Parent as SponsorsLocation, COUNT(c.sponsor) AS TrialCount
    FROM clinicaltrial c
    JOIN Pharmaceuticals p ON c.sponsor = p.Parent_Company
    GROUP BY p.HQ_Country_of_Parent
    ORDER BY TrialCount 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##VisualiSe the result using a horizontal line plot 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC  
-- MAGIC # run the SQL query and retrieve the results as a Spark DataFrame
-- MAGIC df = spark.sql(""" SELECT p.HQ_Country_of_Parent as SponsorsLocation, COUNT(c.sponsor) AS TrialCount
-- MAGIC     FROM clinicaltrial c
-- MAGIC     JOIN Pharmaceuticals p ON c.sponsor = p.Parent_Company
-- MAGIC     GROUP BY p.HQ_Country_of_Parent
-- MAGIC     ORDER BY TrialCount  """)
-- MAGIC  
-- MAGIC # convert the Spark DataFrame to a Pandas DataFrame
-- MAGIC pandas_df = df.toPandas()
-- MAGIC  
-- MAGIC #plot the data using Matplotlib
-- MAGIC plt.figure(figsize=(10,7))
-- MAGIC plt.bar(pandas_df['SponsorsLocation'], pandas_df['TrialCount'])
-- MAGIC plt.xlabel("Sponsor's Location")
-- MAGIC plt.ylabel("Trial Count")
-- MAGIC plt.title("Clinical Trial Count by Sponsor's Location")
-- MAGIC  
-- MAGIC #rotate the x-axis tick labels
-- MAGIC plt.xticks(rotation=90)
-- MAGIC  
-- MAGIC plt.show()
