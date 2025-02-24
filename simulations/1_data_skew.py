from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, rand, sum, when, regexp_replace
import random
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkProblems-DataSkew") \
    .getOrCreate()
    # .config("spark.driver.memory", "4g") \


# Problem 1: Data Skew
def simulate_data_skew():
    logger.info("\n=== Problem 1: Data Skew ===")

    # Create sample data with skewed distribution
    data = []
    for i in range(1000):
        # Creating heavily skewed data where 80% of records have the same key
        key = "KEY_1" if random.random() < 0.8 else f"KEY_{random.randint(2, 10)}"
        data.append((key, random.randint(1, 100)))

    df = spark.createDataFrame(data, ["key", "value"])

    logger.info("Original data distribution:")
    df.groupBy("key").count().show()

    # Problem: Slow group by operation due to data skew
    # Bad approach
    logger.info("\nSlow approach with skewed data:")
    start = time.time()

    df.groupBy("key").agg(sum("value").alias("total")).show()

    logger.info("\nSlow approach with skewed data:")
    logger.info(f"Time taken: {time.time() - start:.2f} seconds")
    # Solution: Salt the keys for better distribution
    logger.info("\nSolution - Salted keys approach:")
    num_partitions = 4

    # Add salt to skewed keys
    df_salted = df.withColumn("salted_key",
                              when(col("key") == "KEY_1",
                                   concat(
                                       col("key"),
                                       (rand() * num_partitions).cast("int").cast("string")
                                    )).otherwise(col("key")))

    # Perform aggregation with salted keys
    start = time.time()
    result = df_salted.groupBy("salted_key").agg(sum("value").alias("total")) \
        .withColumn("original_key", regexp_replace(col("salted_key"), "KEY_1[0-9]", "KEY_1")) \
        .groupBy("original_key").agg(sum("total").alias("final_total"))
    logger.info(f"Time taken: {time.time() - start:.2f} seconds")

    result.show()


if __name__ == "__main__":
    simulate_data_skew()
