import unittest
from collections import Mapping
import pandas as pd
from DataTransformation.FlatFileGenerator import FlatFileGenerator
from ReportingSourceCode.DataLoader.data_file_paths import ec_hc_path


class TestFlatFileGenerator(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.ec_input_data = pd.read_excel(ec_hc_path)
        cls.ffg = FlatFileGenerator(cls.ec_input_data)
        cls.ec_flat_file = cls.ffg.create_flat_file()
        cls.base_organizational_dataframe = cls.ffg.create_base_organizational_dataframe()
        cls.test_name_map_dictionary = cls.ffg.create_org_unit_name_map_dictionary(cls.ffg.ec_input_data)
        cls.test_flat_org_df, cls.test_flat_org_id_df = cls.ffg.create_org_id_df()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.ffg = None

    def test_create_base_organizational_dataframe_returns_more_than_0_rows(self):
        self.assertGreater(self.base_organizational_dataframe.shape[0], 0)

    def test_create_base_organizational_dataframe_returns_more_than_0_columns(self):
        self.assertGreater(self.base_organizational_dataframe.shape[1], 0)

    def test_create_base_organizational_dataframe_returns_dataframe(self):
        self.assertIsInstance(self.base_organizational_dataframe, pd.DataFrame)

    def test_has_department_u_returns_none(self):
        self.assertIsNone(self.ffg.has_department_u(78595555))

    def test_has_function_u_returns_none(self):
        self.assertIsNone(self.ffg.has_function_u(78595555))

    def test_has_management_u_returns_none(self):
        self.assertIsNone(self.ffg.has_management_u(78595555))

    def test_has_department_u_returns_correct_org_id(self):
        self.assertEqual(self.ffg.has_department_u(78595), 50171702)

    def test_has_function_u_returns_correct_org_id(self):
        self.assertEqual(self.ffg.has_function_u(78595), 50171619)

    def test_has_management_u_returns_correct_org_id(self):
        self.assertEqual(self.ffg.has_management_u(78595), 50171610)

    def test_get_parent_dep_unit_returns_empty_list(self):
        self.assertListEqual(self.ffg.get_parent_dep_unit(12345, []), [])

    def test_get_parent_fun_unit_returns_empty_list(self):
        self.assertListEqual(self.ffg.get_parent_fun_unit(12345, []), [])

    def test_get_parent_man_unit_returns_empty_list(self):
        self.assertListEqual(self.ffg.get_parent_man_unit(12345, []), [])

    def test_get_lowest_level_org_returns_integer(self):
        rand_employee = int(self.base_organizational_dataframe.sample(1)[self.ffg.identifier])
        self.assertIsInstance(self.ffg.get_lowest_level_org(rand_employee), int)

    def test_get_parents_returns_list(self):
        rand_employee = int(self.base_organizational_dataframe.sample(1)[self.ffg.identifier])
        rand_org_parents = self.ffg.get_parents(rand_employee, {})
        self.assertIsInstance(rand_org_parents, list)

    def test_get_parents_returns_list_with_len_greater_than_0(self):
        rand_employee = int(self.base_organizational_dataframe.sample(1)[self.ffg.identifier])
        rand_org_parents = self.ffg.get_parents(rand_employee, {})
        self.assertGreater(len(rand_org_parents), 0)

    def test_create_name_mapping_input_dataframe_returns_dataframe(self):
        df = self.ffg.create_name_mapping_input_dataframe(self.ffg.ec_input_data)
        self.assertIsInstance(df, pd.DataFrame)

    def test_create_org_unit_name_map_dictionary_returns_mapping(self):
        self.assertIsInstance(self.test_name_map_dictionary, Mapping)

    def test_create_org_unit_name_map_dictionary_returns_mapping_with_len_greater_than_0(self):
        self.assertGreater(len(self.test_name_map_dictionary), 0)

    def test_map_org_names_to_flat_org_structure_returns_correct_dimensions(self):
        organizational_dict = {}
        employee_list = list(self.ffg.orgs_for_ff[self.ffg.identifier])
        organizational_line = [self.ffg.get_parents(employee, organizational_dict) for employee in employee_list]
        org_dict = {'Org Line': organizational_line}
        flat_org_df = pd.DataFrame(org_dict)
        names = self.ffg.map_org_names_to_flat_org_structure(flat_org_df, self.test_name_map_dictionary)

        self.assertEqual(self.base_organizational_dataframe.shape[0], names.shape[0])

    def test_create_employee_df_returns_correct_dimensions(self):
        employee_df = self.ffg.create_employee_df()
        self.assertEqual(employee_df.shape[0], self.ffg.orgs_for_ff.shape[0])

    def test_create_org_id_df_returnes_tuple_of_dataframes(self):
        self.assertIsInstance(self.test_flat_org_df, pd.DataFrame)
        self.assertIsInstance(self.test_flat_org_id_df, pd.DataFrame)

    def test_create_org_id_df_returns_dataframes_with_equal_row_numbers(self):
        self.assertEqual(self.test_flat_org_df.shape[0], self.test_flat_org_id_df.shape[0])

    def test_create_org_name_df_returns_correct_row_number(self):
        org_name_df = self.ffg.create_org_name_df(self.test_flat_org_df)
        self.assertEqual(org_name_df.shape[0], self.test_flat_org_df.shape[0])

    def test_return_flat_file_returns_correct_row_number(self):
        flat_file = self.ffg.create_flat_file()
        self.assertEqual(flat_file.shape[0], self.test_flat_org_df.shape[0])








