from pyspark.sql import SparkSession

def compare_csv_files_sql(file1: str, file2: str, key_columns: list, other_columns: list):
    # Initialize Spark session
    spark = SparkSession.builder.appName("CSVComparison").getOrCreate()

    # Read the CSV files
    df1 = spark.read.option("header", "true").csv(file1)
    df2 = spark.read.option("header", "true").csv(file2)

    # Create temporary views for SQL queries
    df1.createOrReplaceTempView("table1")
    df2.createOrReplaceTempView("table2")

    # Prepare the keys and columns for SQL queries
    keys_condition = " AND ".join([f"table1.{col} = table2.{col}" for col in key_columns])
    all_columns = key_columns + other_columns

    # SQL to find new rows (rows in table2 but not in table1)
    new_rows_sql = f"""
    SELECT 
        'New' AS type,
        {', '.join([f'table2.{col}' for col in key_columns])} AS key,
        column_name,
        NULL AS old_value,
        new_value
    FROM 
    (
        SELECT {', '.join([f'table2.{col}' for col in key_columns])},
        stack({len(other_columns)}, {', '.join([f"'{col}', table2.{col}" for col in other_columns])}) AS (column_name, new_value)
        FROM table2
        LEFT JOIN table1 ON {keys_condition}
        WHERE {keys_condition} IS NULL
    ) temp
    """

    # SQL to find missed rows (rows in table1 but not in table2)
    missed_rows_sql = f"""
    SELECT 
        'Missed' AS type,
        {', '.join([f'table1.{col}' for col in key_columns])} AS key,
        column_name,
        old_value,
        NULL AS new_value
    FROM 
    (
        SELECT {', '.join([f'table1.{col}' for col in key_columns])},
        stack({len(other_columns)}, {', '.join([f"'{col}', table1.{col}" for col in other_columns])}) AS (column_name, old_value)
        FROM table1
        LEFT JOIN table2 ON {keys_condition}
        WHERE {keys_condition} IS NULL
    ) temp
    """

    # SQL to find changed rows (same key but different column values)
    changed_rows_sql = f"""
    SELECT
        'Changed' AS type,
        {', '.join([f'table1.{col}' for col in key_columns])} AS key,
        column_name,
        old_value,
        new_value
    FROM 
    (
        SELECT {', '.join([f'table1.{col}' for col in key_columns])},
        stack({len(other_columns)}, {', '.join([f"'{col}', table1.{col}, table2.{col}" for col in other_columns])}) AS (column_name, old_value, new_value)
        FROM table1
        JOIN table2 ON {keys_condition}
    ) temp
    WHERE old_value != new_value
    """

    # Run the SQL queries
    new_rows_df = spark.sql(new_rows_sql)
    missed_rows_df = spark.sql(missed_rows_sql)
    changed_rows_df = spark.sql(changed_rows_sql)

    # Union all results
    result_df = new_rows_df.union(missed_rows_df).union(changed_rows_df)

    # Show results and write to CSV
    result_df.show(truncate=False)
    result_df.write.mode("overwrite").option("header", "true").csv("comparison_results_sql.csv")

if __name__ == '__main__':
    file1 = 'file1.csv'
    file2 = 'file2.csv'
    key_columns = ['c1', 'c2', 'c3']  # Define key columns here
    other_columns = ['c4', 'c5']  # Define other columns here
    compare_csv_files_sql(file1, file2, key_columns, other_columns)
