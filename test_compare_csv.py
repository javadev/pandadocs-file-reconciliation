import unittest
import pandas as pd
import os
from io import StringIO
from compare_csv import compare_csv_files  # Assuming your script is named compare_csv.py

class TestCompareCSVFiles(unittest.TestCase):

    def setUp(self):
        # Create example CSV data for testing
        self.file1_data = """c1,c2,c3,c4,c5
1,A,X,10,20
2,B,Y,15,25
3,C,Z,20,30
"""
        self.file2_data = """c1,c2,c3,c4,c5
1,A,X,10,22
2,B,Y,17,25
4,D,W,30,40
"""
        self.file1_path = 'file1.csv'
        self.file2_path = 'file2.csv'
        self.result_path = 'comparison_results.csv'

        # Write the example data to CSV files
        with open(self.file1_path, 'w') as f:
            f.write(self.file1_data)
        with open(self.file2_path, 'w') as f:
            f.write(self.file2_data)

    def tearDown(self):
        # Clean up the files after test
        os.remove(self.file1_path)
        os.remove(self.file2_path)
        if os.path.exists(self.result_path):
            os.remove(self.result_path)

    def test_compare_csv_files(self):
        compare_csv_files(self.file1_path, self.file2_path)

        # Expected result CSV data
        expected_result = """type,key,column_name,old_value,new_value
Changed,1,A,X,c4,10,10
Changed,1,A,X,c5,20,22
Changed,2,B,Y,c4,15,17
New,4,D,W,c4,None,30
New,4,D,W,c5,None,40
Missed,3,C,Z,c4,20,None
Missed,3,C,Z,c5,30,None
"""
        with open(self.result_path, 'r') as f:
            result_data = f.read()

        self.assertEqual(result_data.strip(), expected_result.strip())

if __name__ == '__main__':
    unittest.main()
