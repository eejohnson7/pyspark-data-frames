'''
This time, you'll have the opportunity to apply your knowledge by writing code almost from scratch.

In this task, you'll focus on:

Creating DataFrames from a list and an RDD.
Displaying the contents and printing the schema of the DataFrame created from the list.
Printing the number of rows in the DataFrame created from the RDD.
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("BasicOperations").getOrCreate()

# Create a simple list of tuples representing data
data = [("Alice", 1), ("Bob", 2), ("Cathy", 1)]

# TODO: Create a DataFrame directly from the list
# - Use "Name" and "Value" as column names
df = spark.createDataFrame(data, ["Name", "Value"])

# TODO: Show the contents of the DataFrame created from the list
df.show()

# TODO: Print the schema of the DataFrame created from the list
df.printSchema()

# Convert the list into an RDD
rdd = spark.sparkContext.parallelize(data)

# TODO: Create a DataFrame from the existing RDD
# - Use "Name" and "Value" as column names
df_rdd = spark.createDataFrame(rdd, ["Name", "Value"])

# TODO: Print the number of rows in the DataFrame created from the RDD
print("Number of rows in DataFrame from RDD:", df_rdd.count())

# Stop the SparkSession to release resources
spark.stop()