'''
It's time to dive deeper and make some adjustments.

Your task is to change the CSV loading option from header=True to header=False and observe how this change affects DataFrame 
processing.

Tackling this task will enhance your understanding of how options influence data loading. Give it a try and see the difference 
it makes!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("LoadingDataFrames").getOrCreate()

# TODO: Change the header option to False in the CSV loading
csv_df = spark.read.csv("data.csv", header=False, inferSchema=True)

# Display the inferred schema of the CSV DataFrame
csv_df.printSchema()

# Display the first 3 rows of the CSV DataFrame
csv_df.show(3)

# Stop the SparkSession to release resources
spark.stop()