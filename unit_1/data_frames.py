'''
You've been learning about DataFrames and their functionalities. Now, let's practice by filling in some blanks in a code 
snippet.

Your tasks are to:

Create a DataFrame from the provided list of tuples.
Create another DataFrame, this time from the RDD.
Display the contents of both DataFrames.
This hands-on challenge will boost your skills and confidence. Dive in and enjoy the process!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("BasicOperations").getOrCreate()

# Create a simple list of tuples representing data
data = [("Alice", 1), ("Bob", 2), ("Cathy", 1)]

# TODO: Create a DataFrame directly from the list
df_from_list = spark.createDataFrame(data, ["Name", "Value"])

# TODO: Show the contents of the DataFrame created from the list
df_from_list.show()

# Convert the list into an RDD
rdd = spark.sparkContext.parallelize(data)

# TODO: Create a DataFrame from the existing RDD
df_from_rdd = spark.createDataFrame(rdd, ["Name", "Value"])

# TODO: Show the contents of the DataFrame created from the RDD
df_from_rdd.show()

# Stop the SparkSession to release resources
spark.stop()