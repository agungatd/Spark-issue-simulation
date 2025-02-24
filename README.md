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
   - The Spark master UI will be available at [http://localhost:9090](http://localhost:9090).
   - The first worker’s UI will be available at [http://localhost:8081](http://localhost:8081).

## Running the Simulations

Each simulation script can be submitted to the Spark cluster. For example, to run the Task Serialization Error simulation from your local, use:
```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 simulations/1_data_skew.py
```
Replace `1_data_skew.py` with the appropriate simulation file name. Ensure that the Spark master URL matches your Docker Compose configuration.

## Simulation Examples

The repository covers the following simulations:

1. **Data Skew**  
   *Problem:* When data is unevenly distributed across partitions, causing some executors to process much more data than others.  
   *Solution:* Implemented key salting to distribute skewed keys across multiple partitions.

2. **Out Of Memory Error**  
   *Problem:* Occurs when trying to collect too much data to the driver.  
   *Solution:*
      - Using sampling for analysis
      - Implementing proper aggregations instead of collecting all data

3. **Cartesian Product**
   *Problem:* Unintentional cross joins that can cause exponential data growth.
   *Solution:* Implemented proper join conditions and demonstrated the difference in output size.

4. **Shuffle Partition Issues**  
   *Problem:* Incorrect number of shuffle partitions leading to poor performance.  
   *Solution:* Dynamic partition calculation based on data size. Includes best practices for partition sizing (100MB-200MB per partition)

5. **UDF Performance Issues**  
   *Problem:* Python UDFs incur serialization overhead and reduce performance.  
   *Solution:* Replace UDFs with native Spark SQL functions where possible.

6. **Insufficient Driver Memory and Excessive Garbage Collection**  
   *Problem:* Collecting massive datasets to the driver causes memory exhaustion.  
   *Solution:* Use actions like `take()` to limit data collection.

7. **Shuffle Spill Due to Insufficient Memory**  
   *Problem:* Heavy shuffle operations may lead to spills on disk and performance degradation.  
   *Solution:* Optimize Spark configuration settings to better handle shuffles.

8. **Inefficient Caching Leading to Repeated Computations**  
   *Problem:* Recomputing the same transformations multiple times without caching.  
   *Solution:* Cache intermediate results to avoid redundant computations.

9. **Too Many Small Files Leading to Performance Overhead**  
   *Problem:* Numerous small files create a large number of tasks and I/O overhead.  
   *Solution:* Combine small files by reducing partitions with `coalesce`.

10. **Spark SQL Partition Pruning Not Applied**  
    *Problem:* Inadequate partition filtering leads to unnecessary data reads.  
    *Solution:* Use early filtering or partition-aware reads to enforce pruning.

## Contributing

Contributions are welcome! If you have improvements, additional simulations, or suggestions, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Contact

For any questions or support, please contact [agungatidhira@gmail.com](mailto:agungatidhira@gmail.com).

---