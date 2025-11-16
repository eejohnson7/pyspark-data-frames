'''
You've been doing well with DataFrame operations so far. Let’s practice correcting a common mistake and enhance your 
understanding even further.

Fix the code ensuring that each operation builds upon the previously transformed data.

This task will deepen your understanding of maintaining a proper data transformation flow. Keep it up and make the 
adjustments!
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("BasicOperations").getOrCreate()

# Load a DataFrame from a CSV file with headers
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Select specific columns
selected_df = df.select("Name", "Salary")

# Filter rows based on a condition
filtered_df = selected_df.filter(selected_df.Salary > 3000)

# Update an existent column
updated_df = filtered_df.withColumn("Salary", col("Salary") + 500)

# Add a new column
added_df = updated_df.withColumn("Bonus", col("Salary") * 0.05)

# Display final DataFrame
added_df.show()

# Stop the SparkSession to release resources
spark.stop()