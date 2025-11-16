'''
It's rewarding to see your progress with DataFrame operations. Now, let's continue by filling in the blanks to further 
enhance your skills.

Here's what you need to do:

Use the correct method to select the "Name" and "Salary" columns.
Apply the appropriate filter to process only employees with salaries above $3000.
Update the "Salary" column by adding $500 to each value.
Introduce a new "Bonus" column calculated as 5% of the updated salary.
Give it a go and reinforce your understanding of PySpark transformations!
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("BasicOperations").getOrCreate()

# Load a DataFrame from a CSV file with headers
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# TODO: Select specific columns
selected_df = df.select("Name", "Salary")

# TODO: Filter rows based on a condition
filtered_df = selected_df.filter(df.Salary > 3000)

# TODO: Update an existent column
updated_df = filtered_df.withColumn("Salary", col("Salary") + 500)

# TODO: Add a new column
added_df = updated_df.withColumn("Bonus", col("Salary") * 0.05)

# Display final DataFrame
added_df.show()

# Stop the SparkSession to release resources
spark.stop()