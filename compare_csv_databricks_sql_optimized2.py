from pyspark.sql import SparkSession

def compare_csv_files_sql_no_full_outer(file1: str, file2: str, key_columns: list, other_columns: list):
    # Initialize Spark session
    spark = SparkSession.builder.appName("CSVComparisonNoFullOuter").getOrCreate()

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

    # SQL for new rows (only in table2)
    new_rows_sql = f"""
    SELECT
        {", ".join([f"table2.{col}" for col in key_columns])} AS key,
        'New' AS type,
        stack({len(other_columns)},
            {", ".join([f"'{col}', NULL, table2.{col}" for col in other_columns])}
        ) AS (column_name, old_value, new_value)
    FROM
        table2
    LEFT JOIN table1
    ON {keys_condition}
    WHERE {" AND ".join([f"table1.{col} IS NULL" for col in key_columns])}
    """

    # SQL for missed rows (only in table1)
    missed_rows_sql = f"""
    SELECT
        {", ".join([f"table1.{col}" for col in key_columns])} AS key,
        'Missed' AS type,
        stack({len(other_columns)},
            {", ".join([f"'{col}', table1.{col}, NULL" for col in other_columns])}
        ) AS (column_name, old_value, new_value)
    FROM
        table1
    LEFT JOIN table2
    ON {keys_condition}
    WHERE {" AND ".join([f"table2.{col} IS NULL" for col in key_columns])}
    """

    # SQL for changed rows (exists in both tables, but with different non-key values)
    changed_rows_sql = f"""
    SELECT
        {", ".join([f"table1.{col}" for col in key_columns])} AS key,
        'Changed' AS type,
        stack({len(other_columns)},
            {", ".join([f"'{col}', table1.{col}, table2.{col}" for col in other_columns])}
        ) AS (column_name, old_value, new_value)
    FROM
        table1
    JOIN table2
    ON {keys_condition}
    WHERE {" OR ".join([f"table1.{col} != table2.{col}" for col in other_columns])}
    """

    # Combine all the SQLs into one query using UNION ALL
    combined_sql = f"""
    {new_rows_sql}
    UNION ALL
    {missed_rows_sql}
    UNION ALL
    {changed_rows_sql}
    """

    # Run the optimized SQL query
    result_df = spark.sql(combined_sql)

    # Save the results to a CSV file
    result_df.write.mode("overwrite").option("header", "true").csv("comparison_results_sql_no_full_outer.csv")

    # Show the result (optional)
    result_df.show(truncate=False)

if __name__ == '__main__':
    file1 = 'file1.csv'
    file2 = 'file2.csv'
    key_columns = ['c1', 'c2', 'c3']  # Define key columns here
    other_columns = ['c4', 'c5']  # Define other columns here
    compare_csv_files_sql_no_full_outer(file1, file2, key_columns, other_columns)
