import sys
from pyspark.sql import SparkSession

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Test Delta Lake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Sample Data
    data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
    columns = ["Name", "Id"]

    # Create a DataFrame
    df = spark.createDataFrame(data, columns)

    # Write DataFrame to Delta table
    df.write.format("delta").mode("overwrite").save("/tmp/delta_table")

    # Read the Delta table
    delta_df = spark.read.format("delta").load("/tmp/delta_table")

    # Show the contents of the Delta table
    delta_df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
