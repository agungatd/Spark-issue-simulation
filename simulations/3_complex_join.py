from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, explode
import time

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark Complex Join Issues") \
    .getOrCreate()

print("1. DEMONSTRATING COMPLEX JOIN ISSUES")
print("===================================")

try:
    print("Creating datasets for join operations...")

    # Create first dataset - customer information
    customers_data = [
        (1, "John", "New York", "Gold"),
        (2, "Alice", "Los Angeles", "Silver"),
        (3, "Bob", "Chicago", "Bronze"),
        (4, "Carol", "Houston", "Gold"),
        (5, "David", "Phoenix", "Gold"),
    ] * 100  # Duplicate to increase size

    customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "city", "tier"])

    # Create second dataset - orders information
    orders_data = []
    for i in range(1, 10001):
        customer_id = (i % 500) + 1  # Create a distribution with some repeats
        orders_data.append((i, customer_id, f"Product-{i % 100}", i * 10.0))

    orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "product", "amount"])

    # Create third dataset - product information
    products_data = [(f"Product-{i}", f"Category-{i // 10}", f"Supplier-{i % 5}") for i in range(100)]
    products_df = spark.createDataFrame(products_data, ["product_id", "category", "supplier"])

    # Create fourth dataset - complex nested structure
    transactions_data = []
    for i in range(1, 5001):
        customer_id = (i % 500) + 1
        items = [{"product_id": f"Product-{j}", "quantity": j % 5 + 1, "price": j * 10.0} 
                  for j in range(i % 10 + 1)]
        transactions_data.append((i, customer_id, items))

    transactions_df = spark.createDataFrame(transactions_data, ["transaction_id", "customer_id", "items"])

    print("Sample data:")
    print("Customers:")
    customers_df.show(3)
    print("Orders:")
    orders_df.show(3)
    print("Transactions (with nested structure):")
    transactions_df.show(3, truncate=False)

    # Issue 1: Multi-way joins without optimization
    print("\nIssue 1: Executing a complex multi-way join without optimization...")
    start_time = time.time()

    # Join all tables (except transactions)
    multi_join_result = orders_df \
        .join(customers_df, "customer_id") \
        .join(products_df, orders_df.product == products_df.product_id)

    # Force execution
    multi_join_count = multi_join_result.count()

    multi_join_time = time.time() - start_time

    print(f"Multi-way join completed in {multi_join_time:.2f} seconds")
    print(f"Result had {multi_join_count} rows")

    # Issue 2: Join with nested structure
    print("\nIssue 2: Joining with complex nested data...")
    start_time = time.time()

    # Explode the nested array first
    exploded_transactions = transactions_df.select(
        "transaction_id", 
        "customer_id", 
        explode("items").alias("item")
    )

    # Extract fields from struct
    flattened_transactions = exploded_transactions.select(
        "transaction_id",
        "customer_id",
        col("item.product_id").alias("product_id"),
        col("item.quantity").alias("quantity"),
        col("item.price").alias("price")
    )

    # Join with customers and products
    nested_join_result = flattened_transactions \
        .join(customers_df, "customer_id") \
        .join(products_df, flattened_transactions.product_id == products_df.product_id)

    # Force execution
    nested_join_count = nested_join_result.count()

    nested_join_time = time.time() - start_time

    print(f"Nested data join completed in {nested_join_time:.2f} seconds")
    print(f"Result had {nested_join_count} rows")

    # Issue 3: Join that produces cartesian product
    print("\nIssue 3: Join that creates an unintended cartesian product...")
    start_time = time.time()

    # Create an issue by joining on columns that don't uniquely identify rows
    # Join customers with products with no relationship
    cartesian_join = customers_df.join(products_df, customers_df.tier == products_df.supplier, "left")

    # Force execution
    cartesian_count = cartesian_join.count()

    cartesian_time = time.time() - start_time

    print(f"Cartesian-like join completed in {cartesian_time:.2f} seconds")
    print(f"Result had {cartesian_count} rows - potentially much larger than expected")

except Exception as e:
    print(f"ERROR OCCURRED: {type(e).__name__}: {str(e)}")
    print("\nThis is related to issues with complex joins in Spark")

print("\n2. FIXING THE JOIN ISSUES")
print("===================================")

# Solution 1: Optimize join order and use broadcast for small tables
print("Solution 1: Optimize join order and use broadcast joins")

start_time = time.time()

# Use broadcast for smaller tables
# Rearrange join order (largest to smallest)
optimized_join = orders_df \
    .join(broadcast(customers_df), "customer_id") \
    .join(broadcast(products_df), orders_df.product == products_df.product_id)

# Force execution
optimized_count = optimized_join.count()

optimized_time = time.time() - start_time

print(f"Optimized join completed in {optimized_time:.2f} seconds")
print(f"Improvement: {multi_join_time / optimized_time:.2f}x faster")

# Solution 2: Pre-process complex nested data before joining
print("\nSolution 2: Pre-process nested data efficiently")

start_time = time.time()

# Pre-process and cache the flattened transactions
flattened = transactions_df.select(
    "transaction_id", 
    "customer_id", 
    explode("items").alias("item")
).select(
    "transaction_id",
    "customer_id",
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity"),
    col("item.price").alias("price")
)

# Cache the flattened data for reuse
flattened.cache()
flattened.count()  # Force caching

# Now do the joins with broadcast
nested_optimized = flattened \
    .join(broadcast(customers_df), "customer_id") \
    .join(broadcast(products_df), flattened.product_id == products_df.product_id)

# Force execution
nested_optimized_count = nested_optimized.count()

nested_optimized_time = time.time() - start_time

print(f"Optimized nested join completed in {nested_optimized_time:.2f} seconds")
print(f"Improvement: {nested_join_time / nested_optimized_time:.2f}x faster")

# Solution 3: Avoid cartesian products with proper join conditions
print("\nSolution 3: Avoid cartesian products with proper join conditions")

start_time = time.time()

# Instead of joining with a non-selective condition, use appropriate filtering
# First filter products with the tier we're interested in
filtered_products = products_df.filter(col("supplier").isin("Gold", "Silver", "Bronze"))

# Then join with proper condition
proper_join = customers_df.join(
    broadcast(filtered_products),
    customers_df.tier == filtered_products.supplier
)

# Force execution
proper_join_count = proper_join.count()

proper_join_time = time.time() - start_time

print(f"Proper join (avoiding cartesian) completed in {proper_join_time:.2f} seconds")
print(f"Row count reduced from {cartesian_count} to {proper_join_count}")

# Solution 4: Use appropriate join types
print("\nSolution 4: Use appropriate join types based on requirements")

# Demonstrate different join types and when to use them
print("Appropriate join type examples:")

# Left join when we want to keep all customers even if no orders
left_join = customers_df.join(
    orders_df, 
    on="customer_id", 
    how="left"
)

# Check for customers with no orders (important business insight)
null_orders = left_join.filter(col("order_id").isNull()).select("customer_id").distinct()
print(f"Number of customers with no orders: {null_orders.count()}")

# Inner join when we only want matched records
inner_join = customers_df.join(
    orders_df,
    on="customer_id",
    how="inner"
)

# Right join when we want all orders even if customer data is missing
right_join = customers_df.join(
    orders_df,
    on="customer_id",
    how="right"
)

# Check for orders with missing customer data (data quality issue)
missing_customers = right_join.filter(col("name").isNull()).select("order_id").distinct()
print(f"Number of orders with missing customer data: {missing_customers.count()}")

# Solution 5: Analyze join performance and optimize with explain
print("\nSolution 5: Analyze join plans with explain() to identify issues")

# Example of analyzing the join plan
print("Example explain output for the optimized join:")
optimized_join.explain()

# Show an example of how to analyze the distribution of join keys
print("\nAnalyzing distribution of join keys (important for performance):")
orders_distribution = orders_df.groupBy("customer_id").count().orderBy(col("count").desc())
orders_distribution.show(5)

# Quick check for skew in join keys
max_orders = orders_distribution.agg({"count": "max"}).collect()[0][0]
avg_orders = orders_df.count() / customers_df.select("customer_id").distinct().count()
print(f"Max orders per customer: {max_orders}")
print(f"Average orders per customer: {avg_orders:.2f}")
if max_orders > avg_orders * 10:
    print("WARNING: Possible skew in join key distribution detected!")

# Cleanup
spark.stop()
print("\nSpark session stopped")