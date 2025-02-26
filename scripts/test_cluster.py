import random
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

def create_test_data(spark):
    """Create a test DataFrame with random numbers"""
    data = [(i, random.randint(1, 100)) for i in range(1000000)]
    return spark.createDataFrame(data, ["id", "value"])

def write_test_data(df, path):
    # Ensure directory exists
    os.makedirs(path, exist_ok=True)

    df.write \
        .partitionBy("value") \
        .mode("overwrite") \
        .parquet(path)
        # .option("compression", "snappy") \

def read_test_data(spark, path, schema):
    # Read back with schema validation
    return spark.read \
        .option("mergeSchema", "false") \
        .schema(schema) \
        .parquet(path)

def test_cluster_connectivity():
    """Test basic cluster connectivity and operations"""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ClusterTest") \
        .getOrCreate()
    
    print("1. Successfully created Spark session")
    
    # Get cluster information
    print("\n2. Cluster Information:")
    print(f"Spark version: {spark.version}")
    print(f"Spark master: {spark.sparkContext.master}")
    print(f"Total executors: {len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos())}")
    
    # Create and process test data
    print("\n3. Creating test dataset...")
    df = create_test_data(spark)
    print(f"Created dataset with {df.count():,} rows")
    
    # Perform some transformations
    print("\n4. Performing transformations...")
    results = df.groupBy(
            (col("value") % 10).alias("remainder")
        ).agg(
            count("*").alias("count"),
            sum("value").alias("sum")
        ).orderBy("remainder")
    
    print("\n5. Results sample:")
    results.show(5)
    
    # Test data persistence
    print("\n6. Testing data persistence...")
    test_path = os.getenv("SPARK_TEST_PATH", "hdfs://namenode:9000/user/spark/checkpoints")
    spark.sparkContext.setCheckpointDir(test_path)
    write_test_data(df, test_path)
    read_back = read_test_data(spark, test_path, df.schema)
    print(f"Successfully wrote and read back {read_back.count():,} rows")
    
    # Get execution metrics
    print("\n7. Job metrics:")
    print(f"Number of jobs: {spark.sparkContext.statusTracker().getJobIdsForGroup().__len__()}")
    print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
    
    spark.stop()
    print("\n8. Successfully stopped Spark session")

if __name__ == "__main__":
    test_cluster_connectivity()
