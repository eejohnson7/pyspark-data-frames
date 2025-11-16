'''
It's time to put all your knowledge into action!

Your task is to fill in the missing parts of the code to load data from CSV, JSON, and Parquet files into DataFrames. 
The data files are located at the following paths:
data.csv
data.json
data.parquet

Dive in and witness the power of PySpark DataFrames!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("LoadingDataFrames").getOrCreate()

# TODO: Load a DataFrame from a CSV file at 'data.csv' with headers and schema inference
df_csv = spark.read.csv("data.csv", header=True, inferSchema=True)

# TODO: Display the first 3 rows of the CSV DataFrame
df_csv.show(3)

# TODO: Load a DataFrame from a JSON file at 'data.json'
df_json = spark.read.json("data.json")

# TODO: Display the first 3 rows of the JSON DataFrame
df_json.show(3)

# TODO: Load a DataFrame from a Parquet file at 'data.parquet'
df_p = spark.read.parquet("data.parquet")

# TODO: Display the first 3 rows of the Parquet DataFrame
df_p.show(3)

# Stop the SparkSession to release resources
spark.stop()