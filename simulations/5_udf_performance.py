import logging
import time
import random
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
        .appName("SparkProblems-UDF") \
        .getOrCreate()


def simulate_udf_perf(df):
    def multiply(x):
        # Define a simple Python UDF for multiplication
        return x * 2

    logger.info("\nBad approach - using UDF instead of spark built-in function:")
    start = time.time()
    multiply_udf = F.udf(multiply, IntegerType())
    df_udf = df.withColumn("double_udf", multiply_udf("value"))
    df_udf.show()
    logger.info(f"\nIssue Time taken: {time.time() - start:.2f} seconds")


def solution(df):
    logger.info("\nSolution - optimal shuffle partitions:")
    start = time.time()

    df_builtin = df.withColumn("double_builtin", F.col("value") * 2)
    df_builtin.show()
    logger.info(f"\nSolution Time taken: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    logger.info("\n=== Problem 5: UDF Performance ===")
    # Create sample data
    data = [(i, random.randint(1, 10)) for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "value"])

    simulate_udf_perf(df)
    solution(df)

    logger.info("\n=== End of Problem 5: UDF Performance ===")
