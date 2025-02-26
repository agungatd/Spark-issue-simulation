from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, broadcast, rand, sum, avg, concat, lit
)
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark Shuffling Issues") \
    .getOrCreate()
    # .config("spark.sql.shuffle.partitions", "200")  # Default 200

print("1. DEMONSTRATING EXCESSIVE SHUFFLING ISSUES")
print("===================================")

try:
    print("Creating sample dataset...")

    # Create a moderately sized dataset
    num_rows = 1_000_000
    df = spark.range(0, num_rows)

    # Add some columns to work with
    df = df.withColumn("group_id", (col("id") % 100).cast("integer")) \
           .withColumn("value", rand() * 1000)

    # Cache for comparing different approaches
    df.cache()
    df.count()  # Force caching

    print("Sample data schema:")
    df.printSchema()

    # Example of a poorly written pipeline with excessive shuffling
    print("\nExecuting a pipeline with excessive shuffling...")
    start_time = time.time()

    # Pipeline with multiple shuffle operations
    result1 = df.groupBy("group_id").count()  # Shuffle 1

    result2 = df.groupBy("group_id").agg({"value": "sum"})  # Shuffle 2

    # Join the two aggregations (Shuffle 3)
    result3 = result1.join(result2, "group_id")

    # Additional aggregation (Shuffle 4)
    result4 = df.groupBy("group_id").agg({"value": "avg"})

    # Final join (Shuffle 5)
    final_result = result3.join(result4, "group_id")

    # Force execution
    final_count = final_result.count()

    excessive_shuffle_time = time.time() - start_time

    print(f"Pipeline with excessive shuffling completed in {excessive_shuffle_time:.2f} seconds")
    print(f"Result had {final_count} rows")

except Exception as e:
    print(f"ERROR OCCURRED: {type(e).__name__}: {str(e)}")
    print("\nThis could be related to excessive shuffling")

print("\n2. FIXING THE SHUFFLING ISSUES")
print("===================================")

# Solution 1: Combine operations to reduce shuffles
print("Solution 1: Combine operations to reduce shuffles")
start_time = time.time()

# Perform all aggregations in a single groupBy operation
optimized_result = df.groupBy("group_id").agg(
    count("*").alias("count"),
    sum("value").alias("sum_value"),
    avg("value").alias("avg_value")
)

optimized_count = optimized_result.count()
optimized_time = time.time() - start_time

print(f"Optimized pipeline completed in {optimized_time:.2f} seconds")
print(f"Result had {optimized_count} rows")
print(f"Improvement: {excessive_shuffle_time / optimized_time:.2f}x faster")

# Solution 2: Control the number of partitions
print("\nSolution 2: Control the number of partitions")
print("Current shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# Set to a more appropriate value based on data size
spark.conf.set("spark.sql.shuffle.partitions", "20")  # Reduced for this example
print("Adjusted shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

start_time = time.time()
partition_optimized_result = df.groupBy("group_id").agg(
    count("*").alias("count"),
    sum("value").alias("sum_value"),
    avg("value").alias("avg_value")
)
partition_optimized_count = partition_optimized_result.count()
partition_optimized_time = time.time() - start_time

print(f"Partition-optimized pipeline completed in {partition_optimized_time:.2f} seconds")
print(f"Improvement: {optimized_time / partition_optimized_time:.2f}x faster than previous optimization")

# Solution 3: Use broadcast joins for small dataframes
print("\nSolution 3: Use broadcast joins for small dataframes")

# Create a small lookup table
lookup_df = spark.range(0, 100).withColumn("group_name", concat(lit("Group "), col("id").cast("string")))

# Regular join (causes shuffle)
start_time = time.time()
regular_join = df.join(lookup_df, df.group_id == lookup_df.id)
regular_join_count = regular_join.count()
regular_join_time = time.time() - start_time

# Broadcast join (avoids shuffle)
start_time = time.time()
broadcast_join = df.join(broadcast(lookup_df), df.group_id == lookup_df.id)
broadcast_join_count = broadcast_join.count()
broadcast_join_time = time.time() - start_time

print(f"Regular join time: {regular_join_time:.2f} seconds")
print(f"Broadcast join time: {broadcast_join_time:.2f} seconds")
print(f"Improvement: {regular_join_time / broadcast_join_time:.2f}x faster")

# Solution 4: Use repartitioning strategically
print("\nSolution 4: Use repartitioning strategically")

# If you know you'll perform operations on a specific column multiple times
# Repartition once by that column
start_time = time.time()
repartitioned_df = df.repartition("group_id")  # Single shuffle operation

# Then perform multiple operations without additional full shuffles
result1 = repartitioned_df.groupBy("group_id").count()
result2 = repartitioned_df.groupBy("group_id").agg({"value": "sum"})
result3 = repartitioned_df.groupBy("group_id").agg({"value": "avg"})

# Collect all results
result1.count()
result2.count()
result3.count()

repartition_strategy_time = time.time() - start_time
print(f"Repartitioning strategy time: {repartition_strategy_time:.2f} seconds")

# Solution 5: Persist intermediate results that are used multiple times
print("\nSolution 5: Persist intermediate results that are used multiple times")

# Cleanup
spark.stop()
print("\nSpark session stopped")