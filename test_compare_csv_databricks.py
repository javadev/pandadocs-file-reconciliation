import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

from compare_csv_databricks import compare_csv_files  # Import the function to test

class TestCSVComparison(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder.appName("TestCSVComparison").getOrCreate()

    def setUp(self):
        # This will run before each test
        self.key_columns = ['c1', 'c2', 'c3']
        self.value_columns = ['c4', 'c5']

    def test_new_rows(self):
        # Create test data for new rows
        data1 = [(1, 'A', 100, None, None), (2, 'B', 200, None, None)]
        data2 = [(1, 'A', 100, 'X', 10), (2, 'B', 200, 'Y', 20)]
        
        df1 = self.spark.createDataFrame(data1, schema=self.key_columns + self.value_columns)
        df2 = self.spark.createDataFrame(data2, schema=self.key_columns + self.value_columns)

        # Call the comparison function
        result_df = compare_csv_files(df1, df2, self.key_columns, self.value_columns)

        # Check if the results contain the expected new rows
        expected = [(1, '1, A, 100', 'c4', None, 'X'),
                    (1, '1, A, 100', 'c5', None, 10),
                    (2, '2, B, 200', 'c4', None, 'Y'),
                    (2, '2, B, 200', 'c5', None, 20)]
        
        self.assertEqual(sorted(result_df.collect()), sorted(expected))

    def test_missed_rows(self):
        # Create test data for missed rows
        data1 = [(1, 'A', 100, 'X', 10), (2, 'B', 200, 'Y', 20)]
        data2 = [(1, 'A', 100, None, None), (2, 'B', 200, None, None)]
        
        df1 = self.spark.createDataFrame(data1, schema=self.key_columns + self.value_columns)
        df2 = self.spark.createDataFrame(data2, schema=self.key_columns + self.value_columns)

        # Call the comparison function
        result_df = compare_csv_files(df1, df2, self.key_columns, self.value_columns)

        # Check if the results contain the expected missed rows
        expected = [(1, '1, A, 100', 'c4', 'X', None),
                    (1, '1, A, 100', 'c5', 10, None),
                    (2, '2, B, 200', 'c4', 'Y', None),
                    (2, '2, B, 200', 'c5', 20, None)]
        
        self.assertEqual(sorted(result_df.collect()), sorted(expected))

    def test_changed_rows(self):
        # Create test data for changed rows
        data1 = [(1, 'A', 100, 'X', 10), (2, 'B', 200, 'Y', 20)]
        data2 = [(1, 'A', 100, 'Z', 15), (2, 'B', 200, 'Y', 25)]
        
        df1 = self.spark.createDataFrame(data1, schema=self.key_columns + self.value_columns)
        df2 = self.spark.createDataFrame(data2, schema=self.key_columns + self.value_columns)

        # Call the comparison function
        result_df = compare_csv_files(df1, df2, self.key_columns, self.value_columns)

        # Check if the results contain the expected changed rows
        expected = [(1, '1, A, 100', 'c4', 'X', 'Z'),
                    (1, '1, A, 100', 'c5', 10, 15),
                    (2, '2, B, 200', 'c5', 20, 25)]
        
        self.assertEqual(sorted(result_df.collect()), sorted(expected))

    def test_no_changes(self):
        # Create test data for no changes
        data1 = [(1, 'A', 100, 'X', 10), (2, 'B', 200, 'Y', 20)]
        data2 = [(1, 'A', 100, 'X', 10), (2, 'B', 200, 'Y', 20)]
        
        df1 = self.spark.createDataFrame(data1, schema=self.key_columns + self.value_columns)
        df2 = self.spark.createDataFrame(data2, schema=self.key_columns + self.value_columns)

        # Call the comparison function
        result_df = compare_csv_files(df1, df2, self.key_columns, self.value_columns)

        # Check if there are no changes in the results
        self.assertEqual(result_df.count(), 0)

    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
