'''
By now, you've gained valuable insight into filling and removing missing values in DataFrames. 
Let's bring it all together by writing some code from scratch.

Here's what you'll need to do:

Fill missing "Name" values with "Unknown" and "Country" values with "Not Specified".
Drop any rows that contain null values from the DataFrame after filling the missing values.
Show the results after each data operation.
Dive in, and soon you'll master these techniques!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("HandlingMissingValues").getOrCreate()

# Load a DataFrame from a CSV file with some missing values
df = spark.read.csv("students.csv", header=True, inferSchema=True)

# TODO: Fill missing "Name" values with "Unknown", and "Country" values with "Not Specified"
df_fill = df.fillna({"Name": "Unknown", "Country": "Not Specified"})

# TODO: Display the DataFrame after filling missing values
df_fill.show()

# TODO: Drop rows with any missing values from the filled DataFrame
df_drop = df_fill.dropna()

# TODO: Display the DataFrame after dropping rows
df_drop.show()

# Stop the SparkSession to release resources
spark.stop()