'''
With the basics of handling missing values mastered, it's time to put that knowledge into action.

In this exercise, complete the code to:

Fill in default values for null entries in the DataFrame.
Remove rows with any missing data.
Dive in, and you'll soon master these essential techniques!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("HandlingMissingValues").getOrCreate()

# Load a DataFrame from a CSV file with some missing values
df = spark.read.csv("students.csv", header=True, inferSchema=True)

# TODO: Fill missing values with specified default values
df_fill = df.fillna({"Name": "Unknown", "Score": 0})

# Show the DataFrame after filling missing values
df_fill.show()

# TODO: Drop rows from the DataFrame that contain any null values
df_drop = df_fill.dropna()

# Display the DataFrame after dropping rows with missing values
df_drop.show()

# Stop the SparkSession to release resources
spark.stop()