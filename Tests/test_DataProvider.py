import unittest
import pandas as pd
import sys

from ReportingSourceCode.DataLoader.DataLoader import DataLoader
from ReportingSourceCode.DataProvider.DataProvider import DataProvider
from ReportingSourceCode.DataLoader.data_file_paths import region_path, ec_hc_path


class TestDataProvider(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.DataLoader = DataLoader()
        cls.DataProvider = DataProvider(cls.DataLoader)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.DataLoader = None
        cls.DataProvider = None

    def test_get_region_data_returns_dataframe(self):
        self.assertIsInstance(self.DataProvider.get_region_data(), pd.DataFrame)

    def test_get_expanded_ec_data_returns_dataframe(self):
        self.assertIsInstance(self.DataProvider.get_expanded_ec_data(), pd.DataFrame)
        self.assertNotEqual(id(self.DataProvider.get_expanded_ec_data()), id(self.DataLoader.expanded_ec_df))

    def test_get_headcount_data_returns_dataframe(self):
        self.assertIsInstance(self.DataProvider.get_headcount_data(), pd.DataFrame)
        self.assertNotEqual(id(self.DataProvider.get_headcount_data()), id(self.DataLoader.ec_headcount_df))

    def test_get_turnover_data_returns_dataframe(self):
        self.assertIsInstance(self.DataProvider.get_turnover_data(), pd.DataFrame)
        self.assertNotEqual(id(self.DataProvider.get_turnover_data()), id(self.DataLoader.ec_turnover_df))

    def test_get_salary_data_returns_dataframe(self):
        self.assertIsInstance(self.DataProvider.get_salary_data(), pd.DataFrame)
        self.assertNotEqual(id(self.DataProvider.get_salary_data()), id(self.DataLoader.ec_salary_df))

    def test_get_rehire_data_returns_dataframe(self):
        self.assertIsInstance(self.DataProvider.get_rehire_data(), pd.DataFrame)
        self.assertNotEqual(id(self.DataProvider.get_rehire_data()), id(self.DataLoader.ec_rehire_df))

    def test_get_flat_file_returns_dataframe(self):
        self.assertIsInstance(self.DataProvider.get_flat_file(), pd.DataFrame)
        self.assertNotEqual(id(self.DataProvider.get_flat_file()), id(self.DataLoader.ec_flat_file))

    def test_get_manager_line_data_returns_dataframe(self):
        self.assertIsInstance(self.DataProvider.get_manager_line_data(), pd.DataFrame)
        self.assertNotEqual(id(self.DataProvider.get_manager_line_data()), id(self.DataLoader.manager_line_df))

    def test_get_previous_month_headcount_data_dataframe(self):
        self.assertIsInstance(self.DataProvider.get_previous_month_headcount_data(), pd.DataFrame)
        self.assertNotEqual(id(self.DataProvider.get_previous_month_headcount_data()),
                            id(self.DataLoader.previous_month_ec_headcount_df))





