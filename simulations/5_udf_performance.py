from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, element_at, pandas_udf, lit, concat
from pyspark.sql.types import StringType, DoubleType
# import pandas as pd
# import numpy as np
import time
import math

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark UDF Performance Issues") \
    .getOrCreate()

print("1. DEMONSTRATING UDF PERFORMANCE ISSUES")
print("===================================")

try:
    print("Creating sample dataset...")

    # Generate a simple dataset
    num_rows = 1000000
    df = spark.range(0, num_rows)

    # Add more data to work with
    df = df.withColumn("value", (col("id") * 12.34) % 100) \
           .withColumn("text", concat(lit("Item-"), col("id").cast("string")))

    print("Sample data created. Schema:")
    df.printSchema()

    # Define a simple Python UDF for calculation
    def calculate_score(value):
        # Simulate a moderately complex calculation
        if value is None:
            return None

        # Add some computational complexity
        result = math.sin(value) * math.cos(value) * math.sqrt(abs(value)) + math.log(value + 1)
        return result

    # Register the UDF
    calculate_score_udf = udf(calculate_score, DoubleType())

    # Define another UDF for text processing
    def process_text(text):
        if text is None:
            return None

        # Simulate text processing
        words = text.split('-')
        return '-'.join([w.upper() if i % 2 == 0 else w.lower() for i, w in enumerate(words)])

    process_text_udf = udf(process_text, StringType())

    # Time the UDF execution
    print("\nApplying regular Python UDFs to dataset...")
    start_time = time.time()

    # Apply both UDFs
    result_df = df.withColumn("score", calculate_score_udf(col("value"))) \
                  .withColumn("processed_text", process_text_udf(col("text")))

    # Force execution
    result_count = result_df.count()

    udf_time = time.time() - start_time

    print(f"Regular Python UDF execution took {udf_time:.2f} seconds")
    print(f"Processed {result_count} rows")

    # Show sample output
    print("\nSample output from UDFs:")
    result_df.show(5)

except Exception as e:
    print(f"ERROR OCCURRED: {type(e).__name__}: {str(e)}")
    print("\nThis is related to UDF performance issues")

print("\n2. FIXING THE UDF PERFORMANCE ISSUES")
print("===================================")

# # Solution 1: Use Pandas UDFs for vectorized execution
# print("Solution 1: Use Pandas UDFs for vectorized execution")

# # Define Pandas UDF for calculation
# @pandas_udf(DoubleType())
# def pandas_calculate_score(values: pd.Series) -> pd.Series:
#     # Vectorized calculation
#     return (np.sin(values) * np.cos(values) * np.sqrt(np.abs(values)) + np.log(values + 1))

# # Define Pandas UDF for text processing  
# @pandas_udf(StringType())
# def pandas_process_text(texts: pd.Series) -> pd.Series:
#     result = []
#     for text in texts:
#         if text is None:
#             result.append(None)
#         else:
#             words = text.split('-')
#             result.append('-'.join([w.upper() if i % 2 == 0 else w.lower() for i, w in enumerate(words)]))
#     return pd.Series(result)

# # Time the Pandas UDF execution
# print("\nApplying Pandas UDFs to dataset...")
# start_time = time.time()

# # Apply both Pandas UDFs
# pandas_result_df = df.withColumn("score", pandas_calculate_score(col("value"))) \
#                      .withColumn("processed_text", pandas_process_text(col("text")))

# # Force execution
# pandas_result_count = pandas_result_df.count()

# pandas_udf_time = time.time() - start_time

# print(f"Pandas UDF execution took {pandas_udf_time:.2f} seconds")
# print(f"Improvement: {udf_time / pandas_udf_time:.2f}x faster")

# # Show sample output
# print("\nSample output from Pandas UDFs:")
# pandas_result_df.show(5)

# Solution 1: Use built-in functions instead of UDFs when possible
print("\nSolution 1: Use built-in functions instead of UDFs")

# For many operations, Spark SQL functions can be used instead of UDFs
# Note: We can't fully replicate the custom score calculation with built-ins,
# but many simpler operations can be done with built-ins
start_time = time.time()

# Use built-in functions for text processing
# This doesn't exactly match our UDF but shows the approach
from pyspark.sql.functions import upper, lower, split, concat_ws

# Extract parts
built_in_df = df.withColumn("text_parts", split(col("text"), "-"))
built_in_df = built_in_df.withColumn("part1", upper(element_at(col("text_parts"), 1)))
built_in_df = built_in_df.withColumn("part2", lower(element_at(col("text_parts"), 2)))
built_in_df = built_in_df.withColumn("processed_text", concat_ws("-", col("part1"), col("part2")))
built_in_df = built_in_df.drop("text_parts", "part1", "part2")

# For numeric processing, use built-in math functions where possible
from pyspark.sql.functions import sin, cos, sqrt, abs as spark_abs, log

# This is a simplified approximation of our calculation
built_in_df = built_in_df.withColumn("simple_score", 
                                    (sin(col("value")) * cos(col("value")) * 
                                     sqrt(spark_abs(col("value"))) + log(col("value") + 1)))

# Force execution
built_in_count = built_in_df.count()

built_in_time = time.time() - start_time

print(f"Built-in functions execution took {built_in_time:.2f} seconds")
print(f"Improvement over regular UDF: {udf_time / built_in_time:.2f}x faster")

# Show sample output
print("\nSample output from built-in functions:")
built_in_df.show(5)

# Solution 2: Use SQL expressions for performance
print("\nSolution 2: Use SQL expressions directly")

# Register the DataFrame as a temp view for SQL
df.createOrReplaceTempView("data_table")

# Run SQL with expressions
start_time = time.time()

sql_result = spark.sql("""
    SELECT 
        *, 
        SIN(value) * COS(value) * SQRT(ABS(value)) + LN(value + 1) AS score,
        CONCAT(UPPER(SPLIT(text, '-')[0]), '-', LOWER(SPLIT(text, '-')[1])) AS processed_text
    FROM data_table
""")

sql_count = sql_result.count()

sql_time = time.time() - start_time

print(f"SQL expressions execution took {sql_time:.2f} seconds")
print(f"Improvement over regular UDF: {udf_time / sql_time:.2f}x faster")

# Show sample output
print("\nSample output from SQL expressions:")
sql_result.show(5)

# Solution 3: Optimize UDF with caching for repeated values
print("\nSolution 3: Optimize Python UDF with caching for repeated values")

# Create a UDF with internal caching for repeated values
def create_cached_udf(func):
    cache = {}

    def cached_func(x):
        if x not in cache:
            cache[x] = func(x)
        return cache[x]

    return cached_func

# Create cached versions of our UDFs
cached_calculate = create_cached_udf(calculate_score)
cached_calculate_udf = udf(cached_calculate, DoubleType())

cached_process = create_cached_udf(process_text)
cached_process_udf = udf(cached_process, StringType())

# Time the cached UDF execution on a dataset with more repetition
# For demo purposes, create a dataset with repeated values
repeat_df = df.withColumn("value", (col("id") % 100).cast("double"))

start_time = time.time()

cached_result_df = repeat_df.withColumn("score", cached_calculate_udf(col("value"))) \
                           .withColumn("processed_text", cached_process_udf(col("text")))

cached_count = cached_result_df.count()

cached_time = time.time() - start_time

print(f"Cached UDF execution took {cached_time:.2f} seconds on dataset with repeated values")
print(f"Improvement over regular UDF: {udf_time / cached_time:.2f}x faster")

# Solution 4: Use code generation for complex custom logic
print("\nSolution 4: Use Scala UDFs or code generation for maximum performance")
print("For production systems with critical performance requirements:")
print("1. Implement UDFs in Scala instead of Python")
print("2. Use Spark's Catalyst code generation by registering Scala functions")
print("3. Consider implementing as a custom Spark operator for critical paths")

print("\nSolution 5 [!Not Implemented]: Use Pandas UDFs for vectorized execution")

# Cleanup
spark.stop()
print("\nSpark session stopped")
