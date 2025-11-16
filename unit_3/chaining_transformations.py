'''
In this task, you'll practice chaining multiple DataFrame operations into a single transformation pipeline.

Modify the code to combine the separate DataFrame operations into a single chained operation. 
This involves selecting specific columns, filtering, updating an existing column, and adding a new column all in one 
continuous chain.

Give this task a shot to see how modifying filter conditions can impact data manipulation.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("BasicOperations").getOrCreate()

# Load a DataFrame from a CSV file with headers
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

'''
# Select specific columns
selected_df = df.select("Name", "Salary")

# Filter rows based on a condition
filtered_df = selected_df.filter(df.Salary > 2500)

# Update an existent column
updated_df = filtered_df.withColumn("Salary", col("Salary") + 300)

# Add a new column
added_df = updated_df.withColumn("Bonus", col("Salary") * 0.1)
'''

added_df = df.select("Name", "Salary").filter(df.Salary > 2500).withColumn("Salary", col("Salary") + 300).withColumn("Bonus", col("Salary") * 0.1)

# Display final DataFrame
added_df.show()

# Stop the SparkSession to release resources
spark.stop()