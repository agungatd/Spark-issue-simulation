from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, isnan, lit, coalesce

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark Null Handling Issues") \
    .getOrCreate()

print("1. DEMONSTRATING NULL HANDLING ISSUES")
print("===================================")

try:
    print("Creating dataset with null values...")

    # Create a DataFrame with various types of null/missing values
    data = [
        (1, "Alice", 30, 5000.0),
        (2, "Bob", None, 6000.0),
        (3, "Charlie", 35, float('nan')),  # NaN value
        (4, None, 40, 7000.0),
        (5, "Eve", 45, None),
        (6, "Frank", None, None)
    ]

    df = spark.createDataFrame(data, ["id", "name", "age", "salary"])

    print("\nSample data with null values:")
    df.show()

    # Demonstrate common issues with null values

    # Issue 1: Filtering with nulls incorrectly
    print("\nIssue 1: Incorrect filtering with nulls")
    print("Trying to filter rows where age > 35:")

    # This misses null values, which might be unexpected
    filtered_df = df.filter(col("age") > 35)
    print("Result (missing nulls):")
    filtered_df.show()

    # Issue 2: Aggregating with nulls
    print("\nIssue 2: Aggregations with nulls can be misleading")
    avg_age = df.agg({"age": "avg"}).collect()[0][0]
    print(f"Average age: {avg_age}")
    print("Nulls are ignored in aggregations, which may not be what you want")

    # Issue 3: Join problems with nulls
    print("\nIssue 3: Join problems with nulls")

    # Create another dataframe with nulls for demonstration
    other_data = [
        (1, "HR"),
        (2, "Engineering"),
        (None, "Marketing"),  # Null key
        (4, "Sales"),
        (7, "Finance")  # No match
    ]
    other_df = spark.createDataFrame(other_data, ["id", "department"])

    # Join with null values
    joined_df = df.join(other_df, "id")
    print("Join result (nulls don't match):")
    joined_df.show()

    # Issue 4: Equality comparisons with nulls
    print("\nIssue 4: Equality comparisons with nulls")
    nulls_df = df.filter(col("age") is None)  # This doesn't work as expected
    print("Filtering age == None:")
    nulls_df.show()

    # Issue 5: Mathematical operations with nulls
    print("\nIssue 5: Mathematical operations with nulls")
    print("Adding 5 to age column:")
    df.withColumn("age_plus_5", col("age") + 5).show()

except Exception as e:
    print(f"ERROR OCCURRED: {type(e).__name__}: {str(e)}")
    print("\nThis is related to null handling issues in Spark")

print("\n2. FIXING THE NULL HANDLING ISSUES")
print("===================================")

# Solution 1: Proper null filtering
print("Solution 1: Proper null filtering")
print("Correctly filtering for nulls uses isNull() or isNotNull():")

# Correct way to filter nulls
null_ages = df.filter(col("age").isNull())
print("Rows with null ages:")
null_ages.show()

# Correct way to include nulls in comparisons
print("\nRows with age > 35 OR null:")
df.filter((col("age") > 35) | col("age").isNull()).show()

# Solution 2: Handle nulls in aggregations
print("\nSolution 2: Handle nulls in aggregations")
print("Count of non-null ages vs total rows:")
df.select(
    count("*").alias("total_rows"),
    count("age").alias("non_null_ages"),
    count(when(col("age").isNull(), 1)).alias("null_ages")
).show()

# Calculate custom average with default value for nulls
print("\nCustom average with nulls replaced by 0:")
avg_with_zeros = df.select(avg(coalesce(col("age"), lit(0)))).collect()[0][0]
print(f"Average age (with nulls as 0): {avg_with_zeros}")

# Solution 3: Handle nulls in joins
print("\nSolution 3: Handle nulls in joins")
print("Using left join and filling nulls:")

# Fill nulls before joining
cleaned_df = df.fillna({"id": -1})
cleaned_other_df = other_df.fillna({"id": -1})

left_join = cleaned_df.join(cleaned_other_df, "id", "left")
print("Left join after filling nulls:")
left_join.show()

# Solution 4: Proper comparison with nulls
print("\nSolution 4: Proper null comparisons")
print("Correctly filtering for null values:")
correct_nulls_df = df.filter(col("age").isNull())
correct_nulls_df.show()

print("\nFiltering for NaN values correctly:")
nan_df = df.filter(isnan(col("salary")))
nan_df.show()

# Solution 5: Comprehensive null/NaN handling
print("\nSolution 5: Comprehensive null/NaN handling")


# Function to check various types of missing values
def count_missing(df, column_name):
    return df.select(
        count("*").alias("total"),
        count(when(col(column_name).isNull(), 1)).alias("nulls"),
        count(when(isnan(col(column_name)), 1)).alias("nans"),
        count(when(col(column_name) == "", 1)).alias("empty_strings")
    )


print("Missing value analysis for salary column:")
count_missing(df, "salary").show()

# Solution 6: Fill or drop nulls appropriately
print("\nSolution 6: Fill or drop nulls appropriately")

# Fill with different strategies per column
filled_df = df.fillna({
    "name": "Unknown",
    "age": df.agg({"age": "avg"}).collect()[0][0],  # Fill with average
    "salary": 0.0
})

print("Data after filling nulls with appropriate values:")
filled_df.show()

# Drop rows with nulls in critical columns only
critical_cols_df = df.dropna(subset=["id", "name"])
print("\nData after dropping rows with nulls in critical columns:")
critical_cols_df.show()

# Cleanup
spark.stop()
print("\nSpark session stopped")
