from pyspark.sql import SparkSession
import logging
import time
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkProblems-CartesianProduct") \
    .getOrCreate()
    # .config("spark.driver.memory", "4g") \


# Problem 3: Cartesian Product
def simulate_cartesian_product(df1, df2):

    # Problem: Unintentional cartesian product
    logger.info("Bad approach - unintentional cartesian join:")
    start = time.time()
    try:
        # This will create a huge cartesian product
        bad_join = df1.crossJoin(df2)
        logger.info(f"Row count: {bad_join.count()}")
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
    finally:
        logger.info(f"Time taken: {time.time() - start:.2f} seconds")
        return


def solution(df1, df2):
    # Solution: Use proper join condition
    logger.info("\nSolution - Using proper join condition:")
    start = time.time()
    good_join = df1.join(df2, "id", "inner")
    logger.info(f"Row count: {good_join.count()}")
    good_join.show(5)
    logger.info(f"Time taken: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    logger.info("\n=== Problem 3: Cartesian Product ===")
    
    # Create two dataframes
    df1 = spark.createDataFrame([(i, f"value_{i}") for i in range(1000)], ["id", "value1"])
    df2 = spark.createDataFrame([(i, f"value_{i}") for i in range(1000)], ["id", "value2"])
    simulate_cartesian_product(df1, df2)  # comment this if got Error
    solution(df1, df2)
    logger.info("\n=== End of Problem 3: Cartesian Product ===")
