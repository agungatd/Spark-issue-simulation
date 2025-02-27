from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
# from pyspark.sql.functions import col, udf, explode, lit, count, sum, avg, window, expr
# from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType
import random
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BROADCAST VARIABLE MISUSE") \
    .getOrCreate()

print("============== PROBLEM 1: BROADCAST VARIABLE MISUSE ==============")

# Create sample data - large dimension table
print("Creating sample data...")
num_rows = 5_000_000
dimension_data = [(i, f"product_{i}", random.randint(10, 100)) for i in range(num_rows)]
dimension_df = spark.createDataFrame(dimension_data, ["product_id", "product_name", "price"])

# Create small fact table
fact_data = [(i, random.randint(0, num_rows-1), random.randint(1, 10)) 
             for i in range(1000)]
fact_df = spark.createDataFrame(fact_data, ["order_id", "product_id", "quantity"])

# Problem: Inefficient join without broadcast hint
print("Demonstrating the problem - inefficient join without broadcast...")
start_time = time.time()

# BAD APPROACH - no broadcast hint for small table
result_bad = fact_df.join(
    dimension_df,
    fact_df.product_id == dimension_df.product_id
)
result_bad.explain()  # Show the execution plan
# Force execution
result_count = result_bad.count()
base_time = time.time() - start_time
print(f"Non-broadcast count: {result_count:.2f}")
print(f"Time taken without broadcast: {base_time} seconds")

# Solution: Use broadcast hint for smaller table
print("\nImplementing the solution - using broadcast join hint...")
start_time = time.time()

# GOOD APPROACH - use broadcast join hint
result_good = fact_df.join(
    broadcast(dimension_df),
    fact_df.product_id == dimension_df.product_id
)
result_good.explain()  # Show the execution plan with broadcast
# Force execution
result_count = result_good.count()
broadcast_time = time.time() - start_time
print(f"Broadcast count: {result_count}")
print(f"Time taken with broadcast: {broadcast_time:.2f} seconds")
print(f"Improvement: {base_time / broadcast_time:.2f}x faster")

# Clean up
spark.stop()
