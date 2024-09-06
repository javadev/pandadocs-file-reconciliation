import pandas as pd

def compare_csv_files(file1: str, file2: str, key_columns: list, value_columns: list, result_file: str = 'comparison_results.csv'):
    # Read the CSV files
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Merge the dataframes to find new, missed, and changed rows
    merged = df1.merge(df2, on=key_columns, how='outer', suffixes=('_old', '_new'))

    # Create result dataframe
    results = []

    # Check for new rows
    new_rows = merged[merged[[col + '_old' for col in value_columns]].isna().all(axis=1) & merged[[col + '_new' for col in value_columns]].notna().any(axis=1)]
    for _, row in new_rows.iterrows():
        key = ', '.join(f"{row[col]}" for col in key_columns)
        for col in value_columns:
            results.append({'type': 'New', 'key': key, 'column_name': col, 'old_value': None, 'new_value': row[f"{col}_new"]})

    # Check for missed rows
    missed_rows = merged[merged[[col + '_new' for col in value_columns]].isna().all(axis=1) & merged[[col + '_old' for col in value_columns]].notna().any(axis=1)]
    for _, row in missed_rows.iterrows():
        key = ', '.join(f"{row[col]}" for col in key_columns)
        for col in value_columns:
            results.append({'type': 'Missed', 'key': key, 'column_name': col, 'old_value': row[f"{col}_old"], 'new_value': None})

    # Check for changed rows
    for _, row in merged.iterrows():
        key = ', '.join(f"{row[col]}" for col in key_columns)
        for col in value_columns:
            old_value = row[f"{col}_old"]
            new_value = row[f"{col}_new"]
            if pd.notna(old_value) and pd.notna(new_value) and old_value != new_value:
                results.append({'type': 'Changed', 'key': key, 'column_name': col, 'old_value': old_value, 'new_value': new_value})

    # Create a DataFrame for the results
    result_df = pd.DataFrame(results)

    # Save the results to a new CSV file
    result_df.to_csv(result_file, index=False)

if __name__ == '__main__':
    file1 = 'file1.csv'
    file2 = 'file2.csv'
    key_columns = ['c1', 'c2', 'c3']  # Specify your key columns here
    value_columns = ['c4', 'c5']  # Specify your value columns here
    compare_csv_files(file1, file2, key_columns, value_columns)
