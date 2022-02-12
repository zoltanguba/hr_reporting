from ReportingSourceCode.SupportFunctions.filter_dataframes import filter_dataframe_on_legal_company_and_field_name
import unittest
import pandas as pd


class TestSupportFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        path = 'test_inputs/flat_bw_hc_test_input.xlsx'
        cls.df = pd.read_excel(path)

        cls.company = ['GSSE', 'GHU']
        cls.fields = ['Employee', 'Employee name', 'Legal Company code']
        cls.filtered_df = filter_dataframe_on_legal_company_and_field_name(cls.df, cls.company, cls.fields)

    def test_filter_dataframe_on_legal_company_and_field_name_returns_dataframe(self):
        self.assertIsInstance(self.filtered_df, pd.DataFrame)

    def test_filter_dataframe_on_legal_company_and_field_name_returns_correct_legal_companies(self):
        self.assertEqual(list(self.filtered_df['Legal Company code'].unique()), self.company)

    def test_filter_dataframe_on_legal_company_and_field_name_returns_correct_fields(self):
        self.assertEqual(list(self.filtered_df.columns), self.fields)

    def test_filter_dataframe_on_legal_company_and_field_name_throws_exception_when_invalid_dataframe_provided(self):
        self.assertRaises(TypeError, filter_dataframe_on_legal_company_and_field_name,
                          [1, 2, 3], self.company, self.fields)

    def test_filter_dataframe_on_legal_company_and_field_name_throws_exception_when_invalid_company_provided(self):
        self.assertRaises(TypeError, filter_dataframe_on_legal_company_and_field_name,
                          self.filtered_df, 'GSSE', self.fields)

    def test_filter_dataframe_on_legal_company_and_field_name_throws_exception_when_invalid_fields_provided(self):
        self.assertRaises(TypeError, filter_dataframe_on_legal_company_and_field_name,
                          self.filtered_df, self.company, 'Employee')






