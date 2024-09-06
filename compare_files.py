import pandas as pd

def compare_csv_files(file1: str, file2: str):
    # Read the CSV files
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Define the key columns
    key_columns = ['c1', 'c2', 'c3']
    all_columns = ['c1', 'c2', 'c3', 'c4', 'c5']

    # Merge the dataframes to find new, missed, and changed rows
    merged = df1.merge(df2, on=key_columns, how='outer', suffixes=('_old', '_new'))

    # Create result dataframe
    results = []

    # Check for new rows
    new_rows = merged[merged['c4_old'].isna() & merged['c5_old'].isna() & (~merged['c4_new'].isna())]
    for _, row in new_rows.iterrows():
        results.append({'type': 'New', 'key': f"{row['c1']}, {row['c2']}, {row['c3']}", 'column_name': 'c4', 'old_value': None, 'new_value': row['c4_new']})
        results.append({'type': 'New', 'key': f"{row['c1']}, {row['c2']}, {row['c3']}", 'column_name': 'c5', 'old_value': None, 'new_value': row['c5_new']})

    # Check for missed rows
    missed_rows = merged[merged['c4_new'].isna() & merged['c5_new'].isna() & (~merged['c4_old'].isna())]
    for _, row in missed_rows.iterrows():
        results.append({'type': 'Missed', 'key': f"{row['c1']}, {row['c2']}, {row['c3']}", 'column_name': 'c4', 'old_value': row['c4_old'], 'new_value': None})
        results.append({'type': 'Missed', 'key': f"{row['c1']}, {row['c2']}, {row['c3']}", 'column_name': 'c5', 'old_value': row['c5_old'], 'new_value': None})

    # Check for changed rows
    for _, row in merged.iterrows():
        if pd.notna(row['c4_old']) and pd.notna(row['c4_new']) and (row['c4_old'] != row['c4_new']):
            results.append({'type': 'Changed', 'key': f"{row['c1']}, {row['c2']}, {row['c3']}", 'column_name': 'c4', 'old_value': row['c4_old'], 'new_value': row['c4_new']})
        if pd.notna(row['c5_old']) and pd.notna(row['c5_new']) and (row['c5_old'] != row['c5_new']):
            results.append({'type': 'Changed', 'key': f"{row['c1']}, {row['c2']}, {row['c3']}", 'column_name': 'c5', 'old_value': row['c5_old'], 'new_value': row['c5_new']})

    # Create a DataFrame for the results
    result_df = pd.DataFrame(results)

    # Save the results to a new CSV file
    result_df.to_csv('comparison_results.csv', index=False)

if __name__ == '__main__':
    file1 = 'file1.csv'
    file2 = 'file2.csv'
    compare_csv_files(file1, file2)
