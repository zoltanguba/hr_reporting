from ReportingSourceCode.DataLoader.DataLoader import DataLoader
from ReportingSourceCode.DataProvider.DataProvider import DataProvider
from ReportingSourceCode.SupportFunctions.extract_employee_data_changes import extract_employee_data_changes
from ReportDefinitions.paths import output_path
import unittest
import pandas as pd
import os


class test_extract_employee_data_changes(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        actual_path = 'test_inputs/actual_data.xlsx'
        previous_path = 'test_inputs/previous_data.xlsx'
        fields_to_compare = ['User/Employee ID', 'Formal Name',
                             'Job Title', 'Is People Manager']
        cls.actual_df = pd.read_excel(actual_path, nrows=10)
        cls.previous_df = pd.read_excel(previous_path, nrows=10)
        cls.changes_df = extract_employee_data_changes(cls.actual_df, cls.previous_df, fields_to_compare)

    def test_extract_employee_data_changes_returns_dataframe(self):
        self.assertIsInstance(self.changes_df, pd.DataFrame)