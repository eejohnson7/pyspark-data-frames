'''
Nice work on learning DataFrame creation from lists and RDDs. Now, let's dig a bit deeper and optimize our data displays.

In this task, you'll modify the code to show only the first three rows of the DataFrames created from both a list and an RDD. 
To achieve this, you need to pass the desired number of rows to the show() method (e.g., df.show(5) to display the first five 
rows).
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("BasicOperations").getOrCreate()

# Create a simple list of tuples representing data
data = [("Alice", 1), ("Bob", 2), ("Cathy", 1), ("David", 3), ("Eva", 4)]

# Create a DataFrame directly from the list
df_from_list = spark.createDataFrame(data, ["Name", "Value"])

# TODO: Show only the first 3 rows of the DataFrame created from the list
df_from_list.show(3)

# Convert the list into an RDD
rdd = spark.sparkContext.parallelize(data)

# Create a DataFrame from the existing RDD
df_from_rdd = spark.createDataFrame(rdd, ["Name", "Value"])

# TODO: Show only the first 3 rows of the DataFrame created from the RDD
df_from_rdd.show(3)

# Stop the SparkSession to release resources
spark.stop()