'''
Now, let's put your skills to the test by completing a code snippet focused on performing join operations and exporting data.

Here's what you need to do:

Perform the inner, left, and right joins between dept_df and emp_df on the "DeptID" column.
Save the resulting DataFrames using the following formats and file paths:
Inner joined DataFrame as a CSV file at "output/inner_joined_data".
Left joined DataFrame as a JSON file at "output/left_joined_data".
Right joined DataFrame as a Parquet file at "output/right_joined_data".
Keep up the momentum, and continue strengthening your skills!
'''

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.master("local").appName("JoiningAndSavingDataFrames").getOrCreate()

# Read the CSV files into DataFrames
dept_df = spark.read.csv("departments.csv", header=True, inferSchema=True)
emp_df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# TODO: Perform an inner join on the DeptID column
join_df = dept_df.join(emp_df, "DeptID")

# TODO: Display the DataFrame resulting from the inner join
join_df.show()

# TODO: Perform a left join on the DeptID column
left_df = dept_df.join(emp_df, "DeptID", "left")

# TODO: Display the DataFrame resulting from the left join
left_df.show()

# TODO: Perform a right join on the DeptID column
right_df = dept_df.join(emp_df, "DeptID", "right")

# TODO: Display the DataFrame resulting from the right join
right_df.show()

# TODO: Save the inner joined DataFrame as a CSV file "output/inner_joined_data"
join_df.write.csv("output/inner_joined_data")

# TODO: Save the left joined DataFrame as a JSON file "output/left_joined_data"
left_df.write.json("output/left_joined_data")

# TODO: Save the right joined DataFrame as a Parquet file at "output/right_joined_data"
right_df.write.parquet("output/right_joined_data")

# Stop the SparkSession to release resources
spark.stop()