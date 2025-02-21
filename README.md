# Spark Issue Simulation

---

## Overview

This repository provides an end-to-end simulation of 10 common Apache Spark problems and their fixes. It includes sample code demonstrating typical errors—such as Task Serialization errors, Data Skew in Joins, OutOfMemory issues, and more—along with practical fixes. The project is intended as a learning tool to help Spark developers understand and resolve common performance and configuration issues in Spark applications.

## Repository Structure

- **simulations/**: Contains individual PySpark scripts for each of the 10 common Spark issues. Each script includes:
  - The simulation code that triggers a common Spark problem.
  - The corresponding fix to resolve the issue.
- **docker-compose.yml**: A Docker Compose configuration to set up a simple Spark cluster (one master and two workers) for testing the simulations.
- **README.md**: This documentation file.

## Requirements

- **Docker & Docker Compose**: For running the Spark cluster.
- **Python 3.x** and **PySpark**: For executing the simulation scripts locally or within the Docker environment.
- [Optional] **Java and Scala**: Required dependencies for Spark.

## Setup and Running the Docker Cluster

1. **Clone the repository:**
   ```bash
   git clone https://github.com/agungatd/spark-issue-simulation.git
   cd spark-issue-simulation
   ```

2. **Start the Spark cluster using Docker Compose:**
   ```bash
   docker-compose up
   ```
   - The Spark master UI will be available at [http://localhost:8080](http://localhost:8080).
   - The first worker’s UI will be available at [http://localhost:8081](http://localhost:8081).

## Running the Simulations

Each simulation script can be submitted to the Spark cluster. For example, to run the Task Serialization Error simulation, use:
```bash
spark-submit simulations/serialization_error.py --master spark://spark-master:7077
```
Replace `serialization_error.py` with the appropriate simulation file name. Ensure that the Spark master URL matches your Docker Compose configuration.

## Simulation Examples

The repository covers the following simulations:

1. **Task Serialization Error**  
   *Problem:* Non-serializable objects in lambda functions causing errors.  
   *Fix:* Use broadcast variables to safely share values.

2. **Data Skew in Joins**  
   *Problem:* Overloaded partitions during join operations due to skewed key distributions.  
   *Fix:* Introduce a salting mechanism to distribute the keys more evenly.

3. **OutOfMemoryError from Inefficient Grouping**  
   *Problem:* Using `groupByKey` on large datasets leads to memory issues.  
   *Fix:* Replace with `reduceByKey` for local pre-aggregation.

4. **Improper Partitioning Causing Bottlenecks**  
   *Problem:* Too few partitions limit parallelism and slow down processing.  
   *Fix:* Increase partitions using `repartition` or optimize with `coalesce`.

5. **UDF Performance Issues**  
   *Problem:* Python UDFs incur serialization overhead and reduce performance.  
   *Fix:* Replace UDFs with native Spark SQL functions where possible.

6. **Insufficient Driver Memory and Excessive Garbage Collection**  
   *Problem:* Collecting massive datasets to the driver causes memory exhaustion.  
   *Fix:* Use actions like `take()` to limit data collection.

7. **Shuffle Spill Due to Insufficient Memory**  
   *Problem:* Heavy shuffle operations may lead to spills on disk and performance degradation.  
   *Fix:* Optimize Spark configuration settings to better handle shuffles.

8. **Inefficient Caching Leading to Repeated Computations**  
   *Problem:* Recomputing the same transformations multiple times without caching.  
   *Fix:* Cache intermediate results to avoid redundant computations.

9. **Too Many Small Files Leading to Performance Overhead**  
   *Problem:* Numerous small files create a large number of tasks and I/O overhead.  
   *Fix:* Combine small files by reducing partitions with `coalesce`.

10. **Spark SQL Partition Pruning Not Applied**  
    *Problem:* Inadequate partition filtering leads to unnecessary data reads.  
    *Fix:* Use early filtering or partition-aware reads to enforce pruning.

## Contributing

Contributions are welcome! If you have improvements, additional simulations, or suggestions, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Contact

For any questions or support, please contact [agungatidhira@gmail.com](mailto:agungatidhira@gmail.com).

---