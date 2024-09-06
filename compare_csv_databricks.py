from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat_ws, coalesce

# Initialize Spark session
spark = SparkSession.builder.appName("CSVComparison").getOrCreate()

def compare_csv_files(file1: str, file2: str, key_columns: list, value_columns: list, result_path: str = '/dbfs/comparison_results.csv'):
    # Read the CSV files into Spark DataFrames
    df1 = spark.read.option("header", "true").csv(file1)
    df2 = spark.read.option("header", "true").csv(file2)

    # Rename columns for easier comparison after join
    df1 = df1.select([col(c).alias(f"{c}_old") if c in value_columns else col(c) for c in df1.columns])
    df2 = df2.select([col(c).alias(f"{c}_new") if c in value_columns else col(c) for c in df2.columns])

    # Perform an outer join on the key columns
    join_condition = [df1[k] == df2[k] for k in key_columns]
    merged_df = df1.join(df2, join_condition, how="outer")

    # Create the 'key' column for easier tracking of differences
    merged_df = merged_df.withColumn("key", concat_ws(", ", *key_columns))

    # Generate 'New', 'Missed', and 'Changed' rows
    results = []

    # Find new rows
    for col_name in value_columns:
        new_rows = merged_df.filter(col(f"{col_name}_old").isNull() & col(f"{col_name}_new").isNotNull())
        new_rows = new_rows.withColumn("type", lit("New"))
        new_rows = new_rows.withColumn("column_name", lit(col_name))
        new_rows = new_rows.withColumn("old_value", lit(None))
        new_rows = new_rows.withColumn("new_value", col(f"{col_name}_new"))
        results.append(new_rows)

    # Find missed rows
    for col_name in value_columns:
        missed_rows = merged_df.filter(col(f"{col_name}_new").isNull() & col(f"{col_name}_old").isNotNull())
        missed_rows = missed_rows.withColumn("type", lit("Missed"))
        missed_rows = missed_rows.withColumn("column_name", lit(col_name))
        missed_rows = missed_rows.withColumn("old_value", col(f"{col_name}_old"))
        missed_rows = missed_rows.withColumn("new_value", lit(None))
        results.append(missed_rows)

    # Find changed rows
    for col_name in value_columns:
        changed_rows = merged_df.filter(col(f"{col_name}_old").isNotNull() & col(f"{col_name}_new").isNotNull() & (col(f"{col_name}_old") != col(f"{col_name}_new")))
        changed_rows = changed_rows.withColumn("type", lit("Changed"))
        changed_rows = changed_rows.withColumn("column_name", lit(col_name))
        changed_rows = changed_rows.withColumn("old_value", col(f"{col_name}_old"))
        changed_rows = changed_rows.withColumn("new_value", col(f"{col_name}_new"))
        results.append(changed_rows)

    # Union all result dataframes
    result_df = results[0]
    for df in results[1:]:
        result_df = result_df.union(df)

    # Select relevant columns for the result
    result_df = result_df.select("type", "key", "column_name", "old_value", "new_value")

    # Save the result as a CSV file
    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(result_path)

if __name__ == '__main__':
    file1 = '/dbfs/file1.csv'  # Replace with your actual file path
    file2 = '/dbfs/file2.csv'  # Replace with your actual file path
    key_columns = ['c1', 'c2', 'c3']  # Specify your key columns
    value_columns = ['c4', 'c5']  # Specify your value columns
    compare_csv_files(file1, file2, key_columns, value_columns)
