'''
You've done impressive work learning how to join DataFrames and exploring different types of join operations.

Now, test your skills by completing the code snippet below with the missing parts related to DataFrame joins.

Your objectives include:

Inserting the correct method name to execute the join operations.
Filling in the appropriate types of join operations.
Keep going, and soon you'll be handling joins like a pro!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("JoiningAndSavingDataFrames").getOrCreate()

# Read the CSV files into DataFrames
dept_df = spark.read.csv("departments.csv", header=True, inferSchema=True)
emp_df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# TODO: Perform an inner join on the DeptID column
inner_join_df = dept_df.join(emp_df, "DeptID")

# Display the DataFrame resulting from the inner join
inner_join_df.show()

# TODO: Perform a left join on the DeptID column
left_join_df = dept_df.join(emp_df, "DeptID", "left")

# Display the DataFrame resulting from the left join
left_join_df.show()

# TODO: Perform a right join on the DeptID column
right_join_df = dept_df.join(emp_df, "DeptID", "right")

# Display the DataFrame resulting from the right join
right_join_df.show()

# Stop the SparkSession to release resources
spark.stop()