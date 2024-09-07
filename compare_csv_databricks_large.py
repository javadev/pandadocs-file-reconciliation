from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Initialize Spark session
spark = SparkSession.builder.appName("LargeCSVComparison").getOrCreate()

def compare_csv_files_large(df1, df2, key_columns, value_columns):
    # Perform an outer join on the key columns
    merged = df1.join(df2, key_columns, how='outer', suffixes=('_old', '_new'))

    # Iterate over value columns and add `type`, `old_value`, and `new_value` columns directly to the DataFrame
    for column in value_columns:
        merged = merged.withColumn(f'{column}_type', when(
            col(f'{column}_old').isNull() & col(f'{column}_new').isNotNull(), lit('New')
        ).when(
            col(f'{column}_old').isNotNull() & col(f'{column}_new').isNull(), lit('Missed')
        ).when(
            col(f'{column}_old').isNotNull() & col(f'{column}_new').isNotNull() & (col(f'{column}_old') != col(f'{column}_new')), lit('Changed')
        ).otherwise(lit(None)))

    # Select relevant rows where any change has occurred across the value columns
    filters = [col(f'{column}_type').isNotNull() for column in value_columns]
    filtered_df = merged.filter(sum(filters) > 0)

    # Reshape the DataFrame to present the results in a unified format for all columns
    unified_df = None
    for column in value_columns:
        temp_df = filtered_df.select(*key_columns,
                                     lit(column).alias('column_name'),
                                     col(f'{column}_old').alias('old_value'),
                                     col(f'{column}_new').alias('new_value'),
                                     col(f'{column}_type').alias('type')).filter(col('type').isNotNull())
        
        if unified_df is None:
            unified_df = temp_df
        else:
            unified_df = unified_df.union(temp_df)

    return unified_df

# Example usage in Databricks
if __name__ == '__main__':
    # Load CSV files as Spark DataFrames
    df1 = spark.read.csv('/dbfs/path/to/file1.csv', header=True, inferSchema=True)
    df2 = spark.read.csv('/dbfs/path/to/file2.csv', header=True, inferSchema=True)

    # Define key columns and value columns
    key_columns = ['c1', 'c2', 'c3']
    value_columns = ['c4', 'c5']

    # Perform comparison
    result_df = compare_csv_files_large(df1, df2, key_columns, value_columns)

    # Write the result directly to a CSV file in DBFS
    result_df.write.csv('/dbfs/path/to/comparison_results', header=True, mode='overwrite')
