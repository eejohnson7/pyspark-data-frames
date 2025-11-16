'''
Great job mastering DataFrame joins and exports! In this practice, your task is to complete some missing code sections 
that address DataFrame operations in PySpark.

Here's what you need to do:

Perform an inner join on the provided datasets using a shared column.
Display the resulting DataFrame.
Save it as a CSV file at the path:
"output/inner_joined_data".
Completing these steps will solidify your understanding of these crucial operations. You're doing well! Keep pushing forward!
'''
from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("JoiningAndSavingDataFrames").getOrCreate()

# Read the CSV files into DataFrames
dept_df = spark.read.csv("departments.csv", header=True, inferSchema=True)
emp_df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# TODO: Perform an inner join on the DeptID column
joined_df = dept_df.join(emp_df, "DeptID")

# TODO: Display the DataFrame resulting from the inner join
joined_df.show()

# TODO: Save the inner joined DataFrame as a CSV file at the path "output/inner_joined_data"
joined_df.write.csv("output/inner_joined_data")

# Stop the SparkSession to release resources
spark.stop()