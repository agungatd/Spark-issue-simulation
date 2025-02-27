from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, when, col, udf
from pyspark.sql.types import IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("SparkStreamingIssuesSimulation") \
    .getOrCreate()

# Create a streaming DataFrame from the rate source.
# This source generates two columns: 'timestamp' and 'value'.
streamingDF = spark.readStream.format("rate").option("rowsPerSecond", 5).load()


# Define a faulty UDF that will simulate an error when the 'value' equals 5.
def faulty_udf(x):
    if x == 5:
        raise Exception("Simulated error in UDF for value 5")
    return x


faulty_udf_func = udf(faulty_udf, IntegerType())


def process_batch(df, epoch_id):
    print(f"Processing batch {epoch_id}")

    # ---------------------------------------------------
    # Issue 1: Division by Zero Error Simulation
    # ---------------------------------------------------
    try:
        # Intentionally trigger a division by zero error.
        # (value - value) is always 0.
        df_with_error = df.withColumn("div_error", expr("value / (value - value)"))
        df_with_error.show(truncate=False)
    except Exception as e:
        print(f"Division Error in batch {epoch_id}: {e}")
        # Fix: Replace the denominator with a safe value (using CASE WHEN)
        df_fixed = df.withColumn("div_error_fixed", 
                                 expr("value / (CASE WHEN value = 0 THEN 1 ELSE value END)"))
        print("Fixed division error:")
        df_fixed.show(truncate=False)

    # ---------------------------------------------------
    # Issue 2: Schema Mismatch Error Simulation
    # ---------------------------------------------------
    try:
        # Attempt to select a column that does not exist.
        df.select("non_existent").show(truncate=False)
    except Exception as e:
        print(f"Schema Mismatch Error in batch {epoch_id}: {e}")
        # Fix: Select the correct column ('value')
        print("Selecting correct column instead:")
        df.select("value").show(truncate=False)

    # ---------------------------------------------------
    # Issue 3: UDF Error Simulation
    # ---------------------------------------------------
    try:
        # Apply the faulty UDF that will raise an exception for value==5.
        df_udf = df.withColumn("udf_result", faulty_udf_func(col("value")))
        df_udf.show(truncate=False)
    except Exception as e:
        print(f"UDF Error in batch {epoch_id}: {e}")
        # Fix: Use a fallback transformation. For example, for value==5, set udf_result to 0.
        df_udf_fixed = df.withColumn("udf_result", when(col("value") == 5, 0).otherwise(col("value")))
        print("Fixed UDF error:")
        df_udf_fixed.show(truncate=False)


# Start the streaming query with the foreachBatch function.
# Each micro-batch is processed by process_batch(), ensuring errors in one simulation
# don't stop the entire streaming job.
query = streamingDF.writeStream.foreachBatch(process_batch).start()

# Wait for the streaming query to terminate (runs indefinitely until manually stopped)
query.awaitTermination()
