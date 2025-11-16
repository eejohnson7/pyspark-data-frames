'''
In the previous lesson, you learned how to load files into PySpark DataFrames. Now, it's time to practice those skills.

Your task is to fill in the missing parts of the code to load data from CSV, JSON, and Parquet files into DataFrames. 
Focus on using the correct PySpark methods to achieve this.

By completing this exercise, you'll reinforce your understanding and gain confidence in handling these data formats.
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("LoadingDataFrames").getOrCreate()

# TODO: Load a DataFrame from a CSV file with headers and schema inference
csv_df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Display the first 3 rows of the CSV DataFrame
csv_df.show(3)

# TODO: Load a DataFrame from a JSON file
json_df = spark.read.json("data.json")

# Display the first 3 rows of the JSON DataFrame
json_df.show(3)

# TODO: Load a DataFrame from a Parquet file
parquet_df = spark.read.parquet("data.parquet")

# Display the first 3 rows of the Parquet DataFrame
parquet_df.show(3)

# Stop the SparkSession to release resources
spark.stop()