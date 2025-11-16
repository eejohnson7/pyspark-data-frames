'''
Now, let's take it a step further by learning how to load JSON data that's in an array format.

Check out the data_array.json file to see how the data is structured in an array. This means all the data entries are 
grouped within square brackets []. To read such files correctly in PySpark, you need to set the multiLine option to True.

Here's a quick example of how it can be done:
df = spark.read.json("some_data.json", multiLine=True)

Your task is to use this option to modify the JSON loading part in the provided code to accommodate the array structure.
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("LoadingDataFrames").getOrCreate()

# TODO: Change the JSON loading method to read from a file structured as an array
# json_df = spark.read.json("data_array.json")
json_df = spark.read.json("data_array.json", multiLine=True)

# Display the first 3 rows of the JSON DataFrame
json_df.show(3)

# Stop the SparkSession to release resources
spark.stop()