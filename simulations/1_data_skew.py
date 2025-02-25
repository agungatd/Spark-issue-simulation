from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, rand, when, lit, concat, explode, array, desc
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark Data Skew Issues") \
    .getOrCreate()

print("1. DEMONSTRATING DATA SKEW ISSUES")
print("===================================")

# Create skewed data
try:
    print("Creating skewed dataset...")

    # Generate a dataset with severe skew (many rows with the same key)
    num_rows = 1_000_000

    # Create base dataframe
    df = spark.range(0, num_rows)

    # Add a skewed key column - 90% of the data will have the same key value "A"
    skewed_df = df.withColumn(
        "skewed_key",
        when(rand() < 0.9, lit("A"))
        .otherwise(lit("B"))
    )

    # Create another dataframe to join with
    other_df = spark.createDataFrame(
        [("A", "Value A"), ("B", "Value B")],
        ["key", "value"])

    # Show skew stats
    print("\nData distribution:")
    start = time.time()
    skewed_df.groupBy("skewed_key").count().show()
    print(f"Data Skewed distribution check took {time.time() - start:.2f} seconds")

    # Execute a skewed join operation
    print("\nExecuting a join that will suffer from data skew...")
    print("This might be slow due to data skew...")

    # Time the skewed join
    start_time = time.time()

    # Perform the skewed join
    skewed_join = skewed_df.join(other_df, skewed_df.skewed_key == other_df.key)

    # Count to trigger execution
    skewed_count = skewed_join.count()

    skewed_time = time.time() - start_time

    print(f"Skewed join completed with {skewed_count} rows in {skewed_time:.2f} seconds")

except Exception as e:
    print(f"ERROR OCCURRED: {type(e).__name__}: {str(e)}")
    print("\nThis is likely due to data skew causing processing bottlenecks")

print("\n2. FIXING THE DATA SKEW ISSUES")
print("===================================")

# Solution 1: Use salting technique
print("Solution 1: Salting technique to distribute skewed keys")
# Add a salt column to the skewed key
num_salts = 10
print(f"Adding {num_salts} salt values to the skewed key 'A'")

salted_df = skewed_df.withColumn(
    "salted_key",
    when(col("skewed_key") == "A",
         concat(col("skewed_key"), lit("_"), (rand() * num_salts).cast("int")))
    .otherwise(col("skewed_key"))
)

# Create an array column for all rows
salted_other_df = other_df.withColumn(
    "salted_key_array",
    when(col("key") == "A", 
         array([concat(lit("A_"), lit(str(i))) for i in range(num_salts)]))
    .otherwise(array(col("key")))
)

# Explode the array to get one row per salted key
salted_other_df = salted_other_df.withColumn("salted_key", explode("salted_key_array"))

# Join using the salted keys
print("\nExecuting join with salting technique...")
# Time the salted join
start_time = time.time()

# Perform the salted join
salted_join = salted_df.join(salted_other_df, salted_df.salted_key == salted_other_df.salted_key)

# Count to trigger execution
salted_count = salted_join.count()

salted_time = time.time() - start_time

print(f"Salted join completed with {salted_count} rows in {salted_time:.2f} seconds")
print(f"Improvement over original: {skewed_time / salted_time:.2f}x faster")

# Solution 2: Broadcast the smaller table
print("\nSolution 2: Broadcast join for skewed data")
print("This technique works well when one table is small enough to broadcast")

start_time = time.time()
broadcast_join = skewed_df.join(broadcast(other_df), skewed_df.skewed_key == other_df.key)
broadcast_count = broadcast_join.count()
broadcast_time = time.time() - start_time

print(f"Broadcast join completed with {broadcast_count} rows in {broadcast_time:.2f} seconds")
print(f"Improvement over original: {skewed_time / broadcast_time:.2f}x faster")

# Solution 3: Repartitioning
print("\nSolution 3: Repartition before joining")
print("creating ultra skewed data with 99% of the data having the same key value 'A'")
df = spark.range(0, 10_000_000)
ultra_skewed_df = df.withColumn(
        "skewed_key",
        when(rand() < 0.99, lit("A"))
        .otherwise(lit("B"))
    )
# Perform the skewed join
start_time = time.time()
ultra_skewed_join = ultra_skewed_df.join(other_df, ultra_skewed_df.skewed_key == other_df.key)

# Count to trigger execution
ultra_skewed_count = ultra_skewed_join.count()
ultra_skewed_time = time.time() - start_time

# Repartition before joining
start_time = time.time()
repartitioned_join = ultra_skewed_df.repartition(20, "skewed_key").join(
    other_df.repartition(20, "key"),
    ultra_skewed_df.skewed_key == other_df.key
)
repart_count = repartitioned_join.count()
repart_time = time.time() - start_time

# Unpersist the ultra skewed dataframe
ultra_skewed_df.unpersist()

print(f"Repartitioned join completed with {repart_count} rows in {repart_time:.2f} seconds")
print(f"Improvement over original: {ultra_skewed_time / repart_time:.2f}x faster")

# Solution 4: Use a two-stage aggregation for aggregation skew
print("\nSolution 4: Two-stage aggregation for aggregation skew")
print("For skewed aggregations, pre-aggregate with more partitions first:")

# Example with two-stage aggregation - implementing the full solution
start_time = time.time()

# Original skewed aggregation
print("Standard aggregation on skewed data:")
standard_agg = skewed_df.groupBy("skewed_key").count()
standard_agg.show()
standard_agg_time = time.time() - start_time

# Two-stage aggregation approach
start_time = time.time()

# Stage 1: Local pre-aggregation with many partitions
print("Stage 1: Pre-aggregation with many partitions")
local_agg = skewed_df.repartition(50, "skewed_key") \
    .groupBy("skewed_key") \
    .count() \
    .withColumnRenamed("count", "partial_count")

# Stage 2: Global aggregation with fewer partitions
print("Stage 2: Final aggregation with fewer partitions")
global_agg = local_agg.repartition(10) \
    .groupBy("skewed_key") \
    .sum("partial_count") \
    .withColumnRenamed("sum(partial_count)", "total_count")

global_agg.show()
two_stage_time = time.time() - start_time

print(f"Standard aggregation time: {standard_agg_time:.2f} seconds")
print(f"Two-stage aggregation time: {two_stage_time:.2f} seconds")
print(f"Improvement: {standard_agg_time / two_stage_time:.2f}x faster")

# Solution 5: Handle skew by splitting the skewed key processing
print("\nSolution 5: Split processing for skewed keys")

# Identify the skewed keys
skewed_keys = skewed_df.groupBy("skewed_key") \
    .count() \
    .orderBy(desc("count")) \
    .limit(1) \
    .select("skewed_key") \
    .collect()

skewed_key_value = skewed_keys[0]['skewed_key']
print(f"Identified most skewed key: {skewed_key_value}")

# Split processing: handle skewed keys separately from non-skewed keys
start_time = time.time()

# Data with skewed keys
skewed_data = skewed_df.filter(col("skewed_key") == skewed_key_value)
# Data with non-skewed keys
normal_data = skewed_df.filter(col("skewed_key") != skewed_key_value)

# Process skewed part with broadcast join
skewed_result = skewed_data.join(broadcast(other_df), skewed_data.skewed_key == other_df.key)

# Process non-skewed part with regular join
normal_result = normal_data.join(other_df, normal_data.skewed_key == other_df.key)

# Combine results
combined_result = skewed_result.union(normal_result)
split_count = combined_result.count()
split_time = time.time() - start_time

print(f"Split processing completed with {split_count} rows in {split_time:.2f} seconds")
print(f"Improvement over original: {skewed_time / split_time:.2f}x faster")

# Cleanup
spark.stop()
print("\nSpark session stopped")
