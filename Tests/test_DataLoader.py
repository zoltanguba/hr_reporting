import unittest
import pandas as pd

from ReportingSourceCode.DataLoader.DataLoader import DataLoader
from ReportingSourceCode.DataLoader.data_file_paths import region_path, ec_hc_path


class TestDataLoader(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.region_path = region_path
        cls.dataLoader = DataLoader()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.dataLoader = None

    def test_load_region_data_method_returns_dataframe_with_correct_dimensions_and_columns(self):
        region_test_df = pd.read_excel(self.region_path)
        self.assertIsInstance(self.dataLoader.recurring_region_df, pd.DataFrame)
        self.assertEqual(list(self.dataLoader.recurring_region_df.columns), list(region_test_df.columns))
        self.assertEqual(self.dataLoader.recurring_region_df.shape, region_test_df.shape)

    def test_load_ec_data_returns_dataframe_with_correct_shape(self):
        test_df = pd.read_excel(ec_hc_path)
        self.assertEqual(self.dataLoader.raw_ec_df.shape, test_df.shape)

    def test_expand_ec_raw_data_returns_correct_number_of_rows_and_extra_columns(self):
        self.assertEqual(self.dataLoader.raw_ec_df.shape[0], self.dataLoader.expanded_ec_df.shape[0])

        fields = ['Contract Type', 'Age in Years', 'Country Code', 'Days in Contract', 'Is People Manager Y/N',
                  'Seniority in Years']
        for field in fields:
            self.assertTrue(field in self.dataLoader.expanded_ec_df.columns)

    def test_create_ec_headcount_data_has_no_duplication(self):
        ec_hc_df = self.dataLoader.ec_headcount_df
        ec_hc_duplication_count = ec_hc_df[ec_hc_df.duplicated(subset='User/Employee ID', keep=False)].shape[0]
        self.assertEqual(ec_hc_duplication_count, 0)

    def test_create_ec_headcount_data_returns_correct_employee_status(self):
        unique_employee_statuses = list(self.dataLoader.ec_headcount_df['Employee Status'].unique())
        self.assertEqual(len(unique_employee_statuses), 1)
        self.assertEqual(unique_employee_statuses[0], 'Active')

    def test_create_ec_salary_data_returns_correct_number_of_rows(self):
        self.assertEqual(self.dataLoader.ec_salary_df.shape[0], self.dataLoader.raw_ec_df.shape[0])

    def test_create_ec_turnover_data_returns_correct_dimensions(self):
        self.assertGreater(self.dataLoader.ec_turnover_df.shape[0], 0)
        self.assertGreater(self.dataLoader.ec_turnover_df.shape[1], 0)

    def test_create_ec_turnover_data_returns_correct_employee_status(self):
        unique_employee_statuses = list(self.dataLoader.ec_turnover_df['Employee Status'].unique())
        self.assertEqual(len(unique_employee_statuses), 1)
        self.assertEqual(unique_employee_statuses[0], 'Terminated')

    def test_create_ec_rehire_data_returns_correct_dimensions(self):
        self.assertEqual(self.dataLoader.ec_rehire_df.shape[1], self.dataLoader.expanded_ec_df.shape[1])
        self.assertGreater(self.dataLoader.ec_turnover_df.shape[0], 0)

    def test_create_ec_rehire_data_returns_correct_event_type(self):
        unique_event_type = list(self.dataLoader.ec_rehire_df['Event'].unique())
        self.assertEqual(len(unique_event_type), 1)
        self.assertEqual(unique_event_type[0], 'Rehire')

    def test_create_flat_file_input_returns_dataframe(self):
        self.assertIsInstance(self.dataLoader.create_flat_file_input(self.dataLoader.raw_ec_df), pd.DataFrame)

    def test_create_manager_line_data_returns_dataframe(self):
        self.assertIsInstance(self.dataLoader.create_manager_line_data(), pd.DataFrame)

    def test_create_previous_month_ec_headcount_data_returns_dataframe(self):
        self.assertIsInstance(self.dataLoader.create_previous_month_ec_headcount_data(), pd.DataFrame)
