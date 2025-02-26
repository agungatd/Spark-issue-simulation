# Spark Issue Simulation

---

## Overview

This repository provides an end-to-end simulation of 10 common Apache Spark problems and their fixes. It includes sample code demonstrating typical errors such as Task Serialization errors, Data Skew in Joins, OutOfMemory issues, etc. along with the practical fixes. The project is intended as a learning tool to help Spark developers understand and resolve common performance and configuration issues in Spark applications.

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
   git clone https://github.com/agungatd/Spark-issue-simulation.git
   cd Spark-issue-simulation
   ```

2. **Start the Spark cluster using Docker Compose:**
   ```bash
   docker-compose up -d
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
   *Problem:* Uneven data distribution across partitions, causing some executors to process significantly more data than others.  
   *Solution:* Use key salting to distribute skewed data more evenly across partitions.

2. **Out Of Memory Error**  
   *Problem:* Occurs when too much data is collected to the driver, leading to crashes.  
   *Solution:*  
   - Use sampling for analysis instead of collecting full datasets.  
   - Apply aggregations and transformations at the executor level.  

3. **Complex Join Issues**  
   *Problem:* Unintentional cross joins cause exponential data growth.  
   *Solution:* Ensure correct join conditions and leverage broadcast joins where applicable.

4. **Excessive Shuffling Issues**  
   *Problem:* Improper shuffle partitioning degrades performance.  
   *Solution:* Dynamically adjust shuffle partitions based on data size, ensuring each partition is 100MB-200MB.

5. **UDF Performance Issues**  
   *Problem:* Python UDFs introduce serialization overhead, reducing performance.  
   *Solution:* Replace UDFs with native Spark SQL functions or use Pandas UDFs where needed.

6. **Null Values Handling**  
   *Problem:* Null values can cause incorrect aggregations and filtering inefficiencies.  
   *Solution:* Use `fillna()`, `dropna()`, or `coalesce()` to handle missing values efficiently.

7. **Job Lineage Bloating**  
   *Problem:* Excessive transformations and shuffling create long DAG lineage, increasing memory usage.  
   *Solution:* Persist or checkpoint intermediate results to break lineage and improve performance.

8. **Spark Streaming Issues**  
   *Problem:* Recomputing transformations multiple times leads to inefficiency.  
   *Solution:* Cache intermediate results, optimize microbatch intervals, and use watermarking for stateful processing.

9. **Broadcast Variable Misuse**  
   *Problem:* Broadcasting large datasets can overwhelm memory instead of improving performance.  
   *Solution:* Only broadcast small lookup datasets and use `broadcast()` efficiently.

10. **Too Many Small Files Leading to Performance Overhead**  
   *Problem:* Excessive small files generate high task overhead and I/O bottlenecks.  
   *Solution:* Reduce partitions using `coalesce()` or write output with optimized partitioning strategies.

11. **[Placeholder for future issue]**  
   *Problem:* [Describe the problem concisely].  
   *Solution:* [Provide a recommended solution].  

**...and many more to come!**

## Contributing

Contributions are welcome! If you have improvements, additional simulations, or suggestions, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Contact

For any questions or support, please contact [agungatidhira@gmail.com](mailto:agungatidhira@gmail.com).

---