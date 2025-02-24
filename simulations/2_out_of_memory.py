from pyspark.sql import SparkSession
import logging
import time
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkProblems-OOM") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()


# Problem 2: Out of Memory Error
def simulate_oom_error(df):

    # Problem: Collecting large dataset to driver
    logger.info("Bad approach - collecting large dataset to driver:")
    start = time.time()
    try:
        # This might cause OOM
        df.collect()
        logger.info("Successfully collected data")
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
    logger.info(f"Time taken: {time.time() - start:.2f} seconds")


def sampling_solution(df):  
    # Solution 1: Use sampling
    logger.info("\nSolution 1 - Using sampling:")
    start = time.time()
    df.sample(0.01).show(5)
    logger.info(f"Time taken: {time.time() - start:.2f} seconds")


def agg_solution(df):
    # Solution 2: Use aggregation instead of collect
    logger.info("\nSolution 2 - Using aggregation:")
    start = time.time()
    df.groupBy("id").count().show(5)
    logger.info(f"Time taken: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    logger.info("\n=== Problem 2: Out of Memory Error ===")
    # Create large dataset
    large_data = [(random.randint(1,8), f"value_{i}" * 1000) for i in range(17000)]
    df = spark.createDataFrame(large_data, ["id", "large_text"])
    simulate_oom_error(df)  # comment this if got OOM Error
    # sampling_solution(df)
    # agg_solution(df)
    logger.info("\n=== End of Problem 2: Out of Memory Error ===")
