import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
from script import compare_csv_files_large  # Assuming the function is in a file named `script.py`

class TestCSVComparison(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder.master("local[1]").appName("CSVComparisonTests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_new_rows(self):
        # Test for new rows
        data1 = [Row(c1='A', c2='B', c3='C', c4=1, c5=2)]
        data2 = [Row(c1='A', c2='B', c3='C', c4=1, c5=2),
                 Row(c1='D', c2='E', c3='F', c4=3, c5=4)]
        
        df1 = self.spark.createDataFrame(data1)
        df2 = self.spark.createDataFrame(data2)

        result_df = compare_csv_files_large(df1, df2, ['c1', 'c2', 'c3'], ['c4', 'c5'])

        result = result_df.collect()
        self.assertEqual(len(result), 2)  # 2 new columns (c4, c5)
        self.assertEqual(result[0]['type'], 'New')
        self.assertEqual(result[1]['type'], 'New')

    def test_missed_rows(self):
        # Test for missed rows
        data1 = [Row(c1='A', c2='B', c3='C', c4=1, c5=2),
                 Row(c1='D', c2='E', c3='F', c4=3, c5=4)]
        data2 = [Row(c1='A', c2='B', c3='C', c4=1, c5=2)]
        
        df1 = self.spark.createDataFrame(data1)
        df2 = self.spark.createDataFrame(data2)

        result_df = compare_csv_files_large(df1, df2, ['c1', 'c2', 'c3'], ['c4', 'c5'])

        result = result_df.collect()
        self.assertEqual(len(result), 2)  # 2 missed columns (c4, c5)
        self.assertEqual(result[0]['type'], 'Missed')
        self.assertEqual(result[1]['type'], 'Missed')

    def test_changed_rows(self):
        # Test for changed rows
        data1 = [Row(c1='A', c2='B', c3='C', c4=1, c5=2)]
        data2 = [Row(c1='A', c2='B', c3='C', c4=5, c5=2)]
        
        df1 = self.spark.createDataFrame(data1)
        df2 = self.spark.createDataFrame(data2)

        result_df = compare_csv_files_large(df1, df2, ['c1', 'c2', 'c3'], ['c4', 'c5'])

        result = result_df.collect()
        self.assertEqual(len(result), 1)  # 1 changed column (c4)
        self.assertEqual(result[0]['type'], 'Changed')
        self.assertEqual(result[0]['old_value'], 1)
        self.assertEqual(result[0]['new_value'], 5)

    def test_no_changes(self):
        # Test for no changes
        data1 = [Row(c1='A', c2='B', c3='C', c4=1, c5=2)]
        data2 = [Row(c1='A', c2='B', c3='C', c4=1, c5=2)]
        
        df1 = self.spark.createDataFrame(data1)
        df2 = self.spark.createDataFrame(data2)

        result_df = compare_csv_files_large(df1, df2, ['c1', 'c2', 'c3'], ['c4', 'c5'])

        result = result_df.collect()
        self.assertEqual(len(result), 0)  # No changes should be detected

if __name__ == '__main__':
    unittest.main()
