# 7_job_lineage_bloating
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark Lineage Bloating Issues") \
    .getOrCreate()

print("1. DEMONSTRATING LINEAGE BLOATING ISSUES")
print("===================================")

try:
    print("Creating sample dataset...")

    # Generate a simple dataset
    data = [(i, f"value_{i % 100}") for i in range(10000)]
    base_df = spark.createDataFrame(data, ["id", "value"])

    print("Sample data created. Schema:")
    base_df.printSchema()

    # Create a function that simulates incremental transformations
    def add_transformations(df, count):
        result = df
        for i in range(count):
            # Add simple transformations to build up lineage
            result = result.withColumn(f"col_{i}", (col("id") + i) % 100)

            # Add some conditional logic to make the plan more complex
            if i % 10 == 0:
                result = result.withColumn(f"flag_{i}",
                                           when(col(f"col_{i}") > 50, "High")
                                           .otherwise("Low"))
        return result

    # Demonstrate lineage bloating
    print("\nCreating a dataframe with very long transformation lineage...")
    print("This might slow down execution due to long lineage tracking...")

    # Start with a large number of transformations to show the problem
    start_time = time.time()

    # Build a very long lineage (200 transformations)
    long_lineage_df = add_transformations(base_df, 200)

    # Force action to execute the plan and measure performance
    print("Executing action with long lineage...")
    count_result = long_lineage_df.count()

    long_lineage_time = time.time() - start_time

    print(f"Execution with long lineage took {long_lineage_time:.2f} seconds")

    # Show that lineage analysis itself gets slow
    print("\nAnalyzing execution plan with long lineage...")
    plan_analysis_start = time.time()

    # Get and print the execution plan (this can be slow with long lineage)
    plan = long_lineage_df._jdf.queryExecution().logical().toString()

    plan_analysis_time = time.time() - plan_analysis_start
    print(f"Plan analysis took {plan_analysis_time:.2f} seconds")
    newline = '\n'
    print(f"Plan length: {len(plan.split(newline))} lines")

except Exception as e:
    print(f"ERROR OCCURRED: {type(e).__name__}: {str(e)}")
    print("\nThis may be related to lineage bloating where spark tracks too many transformations")

print("\n2. FIXING THE LINEAGE BLOATING ISSUES")
print("===================================")

# Solution 1: Use checkpoint() to truncate lineage
print("Solution 1: Use checkpoint() to truncate lineage")

# Configure checkpoint directory
try:
    checkpoint_dir = "hdfs://namenode:9000/checkpoints"
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
except Exception as e:
    print(f"ERROR OCCURRED: {type(e).__name__}: {str(e)}")
    print("Please setup and run hdfs!")

start_time = time.time()

# Break up the transformations with checkpoints
df_part1 = add_transformations(base_df, 50)
df_part1 = df_part1.checkpoint()  # Truncate lineage here

df_part2 = add_transformations(df_part1, 50)
df_part2 = df_part2.checkpoint()  # Truncate lineage here

df_part3 = add_transformations(df_part2, 50)
df_part3 = df_part3.checkpoint()  # Truncate lineage here

df_final = add_transformations(df_part3, 50)

# Force action
checkpoint_count = df_final.count()

checkpoint_time = time.time() - start_time

print(f"Execution with checkpoints took {checkpoint_time:.2f} seconds")
print(f"Improvement: {long_lineage_time / checkpoint_time:.2f}x faster")

# Plan analysis should be faster now
plan_analysis_start = time.time()
plan = df_final._jdf.queryExecution().logical().toString()
checkpoint_plan_analysis_time = time.time() - plan_analysis_start
print(f"Plan analysis took {checkpoint_plan_analysis_time:.2f} seconds")
print(f"Plan analysis improvement: {plan_analysis_time / checkpoint_plan_analysis_time:.2f}x faster")

# Solution 2: Use cache() or persist() strategically
print("\nSolution 2: Use cache()/persist() strategically")

start_time = time.time()

# Break up transformations with caching
df_step1 = add_transformations(base_df, 50)
df_step1.cache()
df_step1.count()  # Force caching

df_step2 = add_transformations(df_step1, 50)
df_step2.cache()
df_step2.count()  # Force caching

df_step3 = add_transformations(df_step2, 50)
df_step3.cache()
df_step3.count()  # Force caching

df_step_final = add_transformations(df_step3, 50)

cache_count = df_step_final.count()

cache_time = time.time() - start_time

print(f"Execution with caching took {cache_time:.2f} seconds")

# Solution 3: Convert to DataFrame explicitly to cut lineage
print("\nSolution 3: Convert to DataFrame explicitly")

start_time = time.time()

# Add the first 50 transformations
df_temp1 = add_transformations(base_df, 50)

# Convert to a new DataFrame to cut lineage
# In a real scenario, could use repartition or df.write.parquet followed by read
rows = df_temp1.collect()
temp_df = spark.createDataFrame(rows, df_temp1.schema)

# Continue with more transformations on the new DataFrame
df_temp2 = add_transformations(temp_df, 150)

# Force action
conversion_count = df_temp2.count()

conversion_time = time.time() - start_time

print(f"Execution with explicit conversion took {conversion_time:.2f} seconds")

# Solution 4: Better pipeline design
print("\nSolution 4: Better pipeline design with fewer transformations")

start_time = time.time()

# More efficient approach with fewer transformations
# Perform multiple operations in each step
efficient_df = base_df

# Add multiple columns at once
column_exprs = {}
for i in range(50):
    column_exprs[f"col_{i}"] = (col("id") + i) % 100

# Use select to add all columns in one transformation
efficient_df = efficient_df.select("*", *[expr.alias(name) for name, expr in column_exprs.items()])

# Add flag columns in one transformation
flag_exprs = {}
for i in range(5):
    flag_exprs[f"flag_{i}"] = when(col(f"col_{i*10}") > 50, "High").otherwise("Low")

efficient_df = efficient_df.select("*", *[expr.alias(name) for name, expr in flag_exprs.items()])

# Force action
efficient_count = efficient_df.count()

efficient_time = time.time() - start_time

print(f"Execution with efficient pipeline took {efficient_time:.2f} seconds")
print(f"Improvement over original: {long_lineage_time / efficient_time:.2f}x faster")

# Solution 5: Use explain() to analyze and optimize pipelines
print("\nSolution 5: Use explain() to analyze and optimize pipelines")
print("When developing, analyze plans and cut lineage before they get too complex:")

# Example of using explain()
print("\nExecution plan for efficient pipeline:")
efficient_df.explain(extended=False)

# Cleanup
spark.stop()
print("\nSpark session stopped")
