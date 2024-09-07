from pyspark.sql import SparkSession

def compare_csv_files_sql_no_union(file1: str, file2: str, key_columns: list, other_columns: list):
    # Initialize Spark session
    spark = SparkSession.builder.appName("CSVComparisonNoUnion").getOrCreate()

    # Read the CSV files
    df1 = spark.read.option("header", "true").csv(file1).select(key_columns + other_columns)
    df2 = spark.read.option("header", "true").csv(file2).select(key_columns + other_columns)

    # Repartition by key columns for better performance in joins and operations
    df1 = df1.repartition(*key_columns)
    df2 = df2.repartition(*key_columns)

    # Cache the dataframes to avoid recomputation during query execution
    df1.cache()
    df2.cache()

    # Create temporary views for SQL queries
    df1.createOrReplaceTempView("table1")
    df2.createOrReplaceTempView("table2")

    # Prepare keys condition for SQL joins
    keys_condition = " AND ".join([f"table1.{col} = table2.{col}" for col in key_columns])

    # Combined SQL without UNION ALL
    combined_sql = f"""
    SELECT
        {", ".join([f"COALESCE(table1.{col}, table2.{col}) AS {col}" for col in key_columns])} AS key,
        CASE
            WHEN {keys_condition} IS NULL THEN 'New'
            WHEN {keys_condition} IS NOT NULL AND {" AND ".join([f"table2.{col} IS NULL" for col in other_columns])} THEN 'Missed'
            ELSE 'Changed'
        END AS type,
        stack({len(other_columns)},
            {", ".join([f"'{col}', table1.{col}, table2.{col}" for col in other_columns])}
        ) AS (column_name, old_value, new_value)
    FROM
        table1 FULL OUTER JOIN table2
        ON {keys_condition}
    """

    # Run the optimized SQL query
    result_df = spark.sql(combined_sql)

    # Save the results to a CSV file
    result_df.write.mode("overwrite").option("header", "true").csv("comparison_results_sql_no_union.csv")

    # Show the result (optional)
    result_df.show(truncate=False)

if __name__ == '__main__':
    file1 = 'file1.csv'
    file2 = 'file2.csv'
    key_columns = ['c1', 'c2', 'c3']  # Define key columns here
    other_columns = ['c4', 'c5']  # Define other columns here
    compare_csv_files_sql_no_union(file1, file2, key_columns, other_columns)
