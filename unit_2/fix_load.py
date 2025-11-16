'''
You've come a long way with your PySpark skills. Now, let's ensure you can identify and fix common mistakes when loading 
data into DataFrames.

In this practice task, there are bugs in the code related to loading CSV, JSON, and Parquet files. Your task is to correct 
any issues in loading these file types using PySpark.

This hands-on exercise will refine your ability to troubleshoot and ensure accurate data operations. Dive in and show what 
you can do!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("LoadingDataFrames").getOrCreate()

# Load a DataFrame from a CSV file with headers and schema inference
csv_df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Display the first 3 rows of the CSV DataFrame
csv_df.show(3)

# Load a DataFrame from a JSON file
json_df = spark.read.json("data.json")

# Display the first 3 rows of the JSON DataFrame
json_df.show(3)

# Load a DataFrame from a Parquet file
parquet_df = spark.read.parquet("data.parquet")

# Display the first 3 rows of the Parquet DataFrame
parquet_df.show(3)

# Stop the SparkSession to release resources
spark.stop()