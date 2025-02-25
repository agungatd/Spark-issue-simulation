from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
import logging
import time
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a Spark session - intentionally using minimal memory
spark = SparkSession.builder \
    .appName("Spark Memory Issues (OOM)") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

print("1. DEMONSTRATING SPARK MEMORY ISSUES")
print("===================================")

# Create a dataset that will cause memory problems
try:
    print("Creating a large dataset with many columns...")
    
    # Generate a large dataset
    num_rows = 1000000
    
    # Create base dataframe
    df = spark.range(0, num_rows)
    
    # Create many columns to consume memory
    for i in range(100):
        df = df.withColumn(f"random_col_{i}", rand())
    
    # Perform a memory-intensive operation - collecting all data to driver
    print("Attempting to collect() the entire large dataset to driver...")
    result = df.collect()  # This will likely cause an OOM error
    
    print(f"Collected {len(result)} rows with {len(df.columns)} columns")

except Exception as e:
    print(f"ERROR OCCURRED: {type(e).__name__}: {str(e)}")
    print("\nThis is a common Spark memory issue: OutOfMemoryError")

print("\n2. FIXING THE MEMORY ISSUES")
print("===================================")

# Solution 1: Increase memory (in a real cluster, this would be done via configuration)
print("Solution 1: Increase memory allocation")
print("In a real environment, you would increase:")
print("  - spark.driver.memory (for driver)")
print("  - spark.executor.memory (for executors)")
print("  - spark.memory.fraction (memory fraction used for execution)")

# Solution 2: Avoid collect() on large datasets
print("\nSolution 2: Avoid collecting large datasets to driver")
print("Instead of df.collect(), use:")
print("  - df.take(n) to get only n rows")
print("  - df.sample().collect() to get a sample")

# Demonstrate solution with take()
print("\nDemonstrating take(10) instead of collect():")
sample_result = df.take(10)
print(f"Successfully took 10 rows with {len(df.columns)} columns")

# Solution 3: Reduce the number of columns/perform projection earlier
print("\nSolution 3: Reduce the number of columns early in the pipeline")
df_reduced = df.select("id", "random_col_0", "random_col_1")
sample_result_2 = df_reduced.take(10)
print(f"Successfully took 10 rows with only {len(df_reduced.columns)} columns")

# Solution 4: Repartition to avoid data skew
print("\nSolution 4: Repartition to distribute data more evenly")
df_repartitioned = df.repartition(20)  # Distribute to more partitions
print("Data repartitioned to 20 partitions to distribute processing")

# Solution 5: Use disk spilling if needed
print("\nSolution 5: Configure disk spilling for large operations")
print("Set these configurations:")
print("  - spark.memory.storageFraction=0.5")
print("  - spark.memory.offHeap.enabled=true (if available)")
print("  - spark.memory.offHeap.size to allow off-heap storage")

# Cleanup
spark.stop()
print("\nSpark session stopped")
