from pyspark.sql import SparkSession
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Spark Problems Simulation") \
    .getOrCreate()

print("\n============== PROBLEM 2: SMALL FILE PROBLEM ==============")

# Simulate small file problem
print("Creating many small dataframes to simulate small files...")

# Create many small dataframes
small_dfs = []
for i in range(100):
    data = [(i, f"value_{j}") for j in range(100)]
    small_df = spark.createDataFrame(data, ["key", "value"])
    small_dfs.append(small_df)

# Problem: Processing many small files is inefficient
print("Demonstrating the problem - writing many small files...")
start_time = time.time()

# BAD APPROACH - write many small files
for i, df in enumerate(small_dfs):
    df.write.mode("overwrite").parquet(f"/tmp/small_files/part_{i}")

# Reading back all the small files
all_small_files = spark.read \
    .schema(df.schema) \
    .parquet("/tmp/small_files")
small_files_count = all_small_files.count()
unoptimized_time = time.time() - start_time
print(f"Small files count: {small_files_count}")
print(f"Time taken with small files: {unoptimized_time:.2f} seconds")

# Solution: Coalesce/repartition to optimize file sizes
print("\nImplementing the solution - using repartition to control file size...")
# GOOD APPROACH - combine into a single dataframe and control partitions
combined_df = small_dfs[0]
for df in small_dfs[1:]:
    combined_df = combined_df.union(df)

# Repartition to a reasonable number of files
optimal_df = combined_df.repartition(4)

# Write with small number of files
start_time = time.time()
optimal_df.write.mode("overwrite").parquet("/tmp/optimal_files")

# Reading back the optimized files
optimal_files = spark.read \
    .schema(optimal_df.schema) \
    .parquet("/tmp/optimal_files")
optimal_file_count = optimal_files.count()
optimized_time = time.time() - start_time
print(f"Optimal files count: {optimal_file_count}")
print(f"Time taken with optimized files: {optimized_time:.2f} seconds")
print(f"Improvement: {unoptimized_time / optimized_time:.2f}x faster")

# Clean up
spark.stop()
