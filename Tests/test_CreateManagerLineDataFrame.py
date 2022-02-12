import unittest
import pandas as pd
from ReportingSourceCode.DataTransformation.CreateManagerLineDataFrame import CreateManagerLineDataFrame
from ReportingSourceCode.DataLoader.DataLoader import DataLoader
from ReportingSourceCode.DataProvider.DataProvider import DataProvider


class TestCreateManagerLineDataFrame(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.DataLoader = DataLoader()
        cls.DataProvider = DataProvider(cls.DataLoader)
        cls.ec_input_data = cls.DataProvider.get_headcount_data()
        cls.manager_line_generator = CreateManagerLineDataFrame(cls.ec_input_data)
        cls.employee_dict_with_manager_data = cls.manager_line_generator.create_employee_dict_with_manager_data()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.DataLoader = None
        cls.DataProvider = None

    def test_create_employee_dict_with_manager_data_retruns_dictionary(self):
        self.assertIsInstance(self.employee_dict_with_manager_data, dict)

    def test_create_employee_dict_with_manager_data_retruns_dictionary_with_correct_length(self):
        self.assertEqual(len(self.employee_dict_with_manager_data), self.ec_input_data.shape[0])

    def test_get_manager_line_for_employee_returns_list(self):
        rand_employee = int(self.ec_input_data.sample(1)['Global ID'])
        manager_line = self.manager_line_generator.get_manager_line_for_employee(rand_employee)

        self.assertIsInstance(manager_line, list)

    def test_create_emoloyee_id_dataframe_for_manager_line_returns_dataframe_with_correct_row_number(self):
        emoloyee_id_dataframe = self.manager_line_generator.create_emoloyee_id_dataframe_for_manager_line()

        self.assertEqual(emoloyee_id_dataframe.shape[0], self.ec_input_data.shape[0])

    def test_create_manager_line_dataframe_returns_dataframe_with_correct_row_number(self):
        employee_list = list(self.ec_input_data[self.manager_line_generator.identifier])
        manager_line_dataframe = self.manager_line_generator.create_manager_line_dataframe(employee_list)

        self.assertEqual(manager_line_dataframe.shape[0], self.ec_input_data.shape[0])

    def test_create_manager_line_to_employee_dataframe_returns_dataframe_with_correct_row_number(self):
        manager_df = self.manager_line_generator.create_manager_line_to_employee_dataframe()

        self.assertEqual(manager_df.shape[0], self.ec_input_data.shape[0])







