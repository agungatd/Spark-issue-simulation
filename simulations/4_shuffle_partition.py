from pyspark.sql import SparkSession
import logging
import time
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def simulate_shuffle_issues():
    spark = SparkSession.builder \
        .appName("SparkProblems-ShufflePartition-Issue") \
        .getOrCreate()

    data = [(i, random.randint(1, 100)) for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "value"])
    df_count = df.count()

    # Problem: Too few shuffle partitions
    logger.info("\nBad approach - too few shuffle partitions:")
    start = time.time()

    spark.conf.set("spark.sql.shuffle.partitions", "2")
    df.groupBy("id").count().explain()

    logger.info(f"\nIssue Time taken: {time.time() - start:.2f} seconds")
    spark.stop()
    return df_count


def solution(df_cnt):
    # Solution: Adjust number of shuffle partitions
    # Rule of thumb: partition size should be between 100MB-200MB
    num_partitions = max(df_cnt // 100000, 200)  # Assuming each record is ~1KB
    spark = SparkSession.builder \
        .appName("SparkProblems-ShufflePartition-Solution") \
        .config("spark.sql.shuffle.partitions", num_partitions) \
        .getOrCreate()

    data = [(i, random.randint(1, 100)) for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "value"])

    logger.info("\nSolution - optimal shuffle partitions:")
    start = time.time()

    df.groupBy("id").count().explain()

    logger.info(f"\nSolution Time taken: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    logger.info("\n=== Problem 4: Shuffle Partition Issue ===")
    # Create sample data

    df_cnt = simulate_shuffle_issues()
    solution(df_cnt)

    logger.info("\n=== End of Problem 4: Shuffle Partition Issue ===")