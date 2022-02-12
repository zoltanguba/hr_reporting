import pandas as pd
import numpy as np
from typing import Union, Mapping, Tuple


class FlatFileGenerator:
    identifier = 'User/Employee ID'

    def __init__(self, ec_input_data: pd.DataFrame):
        self.ec_input_data = ec_input_data
        self.orgs_for_ff = self.create_base_organizational_dataframe()

    def create_employee_df(self) -> pd.DataFrame:
        employee_list = list(self.orgs_for_ff[self.identifier])
        employee_dict = {self.identifier: employee_list}
        employee_df = pd.DataFrame(employee_dict)

        return employee_df

    def create_base_organizational_dataframe(self) -> pd.DataFrame:
        ff_fields_v2 = [self.identifier, 'Management Unit Parent Management Unit',
                        'Management Unit Management Unit Code',
                        'Function Unit Parent Function Unit', 'Function Unit Function Unit Code',
                        'Department Unit Parent Department Unit', 'Department Unit Department Unit Code']

        base_for_ff = self.ec_input_data[(self.ec_input_data['Employee Status'] != 'Activeeeee') &
                                         (~self.ec_input_data['Management Unit Management Unit Code'].isna())][ff_fields_v2].copy()

        parent_cols = ['Management Unit Parent Management Unit', 'Function Unit Parent Function Unit',
                       'Department Unit Parent Department Unit']
        for col in parent_cols:
            base_for_ff[col] = base_for_ff[col].apply(
                lambda x: str(x)[0:str(x).find("-")] if not pd.isnull(x) else np.nan)

        for col in base_for_ff.columns:
            base_for_ff[col] = pd.to_numeric(base_for_ff[col], errors='coerce')

        orgs_for_ff = base_for_ff
        orgs_for_ff.drop_duplicates(inplace=True)

        return orgs_for_ff

    def has_department_u(self, user: int) -> Union[int, None]:
        try:
            dep_unit = int(list(self.orgs_for_ff.loc[self.orgs_for_ff[self.identifier] == user]['Department Unit Department Unit Code'])[0])
            return dep_unit
        except Exception:
            return

    def has_function_u(self, user: int) -> Union[int, None]:
        try:
            fun_unit = int(list(self.orgs_for_ff.loc[self.orgs_for_ff[self.identifier] == user]['Function Unit Function Unit Code'])[0])
            return fun_unit
        except Exception:
            return

    def has_management_u(self, user: int) -> Union[int, None]:
        try:
            man_unit = int(list(self.orgs_for_ff.loc[self.orgs_for_ff[self.identifier] == user]['Management Unit Management Unit Code'])[0])
            return man_unit
        except Exception:
            return

    def get_parent_dep_unit(self, dep_to_check: int, dep_unit_line: list) -> list:
        try:
            parent_dep_unit = int(list(
                self.orgs_for_ff.loc[(self.orgs_for_ff['Department Unit Department Unit Code'] == dep_to_check)][
                    'Department Unit Parent Department Unit'])[0])
            dep_unit_line.append(parent_dep_unit)
            self.get_parent_dep_unit(parent_dep_unit, dep_unit_line)
        except Exception:
            return dep_unit_line
        return dep_unit_line

    def get_parent_fun_unit(self, fun_to_check: int, dep_unit_line: list) -> list:
        try:
            parent_fun_unit = int(list(self.orgs_for_ff.loc[(self.orgs_for_ff['Function Unit Function Unit Code'] == fun_to_check)][
                                           'Function Unit Parent Function Unit'])[0])
            dep_unit_line.append(parent_fun_unit)
            self.get_parent_fun_unit(parent_fun_unit, dep_unit_line)
        except Exception:
            return dep_unit_line
        return dep_unit_line

    def get_parent_man_unit(self, man_to_check: int, dep_unit_line: list) -> list:
        try:
            parent_man_unit = int(list(
                self.orgs_for_ff.loc[(self.orgs_for_ff['Management Unit Management Unit Code'] == man_to_check)][
                    'Management Unit Parent Management Unit'])[0])
            dep_unit_line.append(parent_man_unit)
            self.get_parent_man_unit(parent_man_unit, dep_unit_line)
        except Exception:
            return dep_unit_line
        return dep_unit_line

    def get_lowest_level_org(self, user: int) -> int:
        dep_unit = self.has_department_u(user)
        if dep_unit:
            return dep_unit
        fun_unit = self.has_function_u(user)
        if fun_unit:
            return fun_unit
        man_unit = self.has_management_u(user)
        if man_unit:
            return man_unit

    def get_parents(self, employee: int, organizational_dict: dict) -> list:
        org_unit_line = []

        lowest_level_org = self.get_lowest_level_org(employee)
        org_dict_check = organizational_dict.get(lowest_level_org)
        if org_dict_check:
            return org_dict_check

        dep_unit = self.has_department_u(employee)
        if dep_unit:
            org_unit_line.append(dep_unit)
            self.get_parent_dep_unit(dep_unit, org_unit_line)

        fun_unit = self.has_function_u(employee)
        if fun_unit:
            org_unit_line.append(fun_unit)
            self.get_parent_fun_unit(fun_unit, org_unit_line)

        man_unit = self.has_management_u(employee)
        if man_unit:
            org_unit_line.append(man_unit)
            self.get_parent_man_unit(man_unit, org_unit_line)

        org_unit_line.reverse()
        organizational_dict[lowest_level_org] = org_unit_line

        return org_unit_line

    def create_name_mapping_input_dataframe(self, ec_input_df: pd.DataFrame) -> pd.DataFrame:
        fields = ['Employee Status', 'Employee Group', self.identifier, 'Formal Name',
                  'Management Unit Management Unit Code', 'Management Unit Management Unit Name',
                  'Management Unit Parent Management Unit',
                  'Function Unit Function Unit Code', 'Function Unit Function Unit Name',
                  'Function Unit Parent Function Unit',
                  'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                  'Department Unit Parent Department Unit']

        # lookup_base_df = ec_input_df[ec_input_df['Employee Status'] != 'Terminated'][fields].copy()
        lookup_base_df = ec_input_df[fields].copy()
        lookup_base_df[self.identifier] = pd.to_numeric(lookup_base_df[self.identifier], errors='coerce')

        parent_cols = ['Management Unit Parent Management Unit', 'Function Unit Parent Function Unit',
                       'Department Unit Parent Department Unit']
        for column in parent_cols:
            level = column.partition(' ')[0]
            lookup_base_df[f'Parent {level} Unit Code'] = lookup_base_df[column].apply(
                lambda x: pd.to_numeric(str(x)[0:str(x).find('-')], errors='coerce'))
            lookup_base_df[f'Parent {level} Unit Name'] = lookup_base_df[column].apply(
                lambda x: str(x)[str(x).find('-') + 1:])

        return lookup_base_df

    def create_org_unit_name_map_dictionary(self, ec_input_df: pd.DataFrame) -> Mapping:
        lookup_base_df = self.create_name_mapping_input_dataframe(ec_input_df)

        org_unit_pairs = [['Management Unit Management Unit Code', 'Management Unit Management Unit Name'],
                          ['Function Unit Function Unit Code', 'Function Unit Function Unit Name'],
                          ['Department Unit Department Unit Code', 'Department Unit Department Unit Name'],
                          ['Parent Management Unit Code', 'Parent Management Unit Name'],
                          ['Parent Function Unit Code', 'Parent Function Unit Name'],
                          ['Parent Department Unit Code', 'Parent Department Unit Name']]

        org_name_lookup_df = pd.DataFrame()

        for pair in org_unit_pairs:
            unit_df = lookup_base_df[[pair[0], pair[1]]].copy()
            unit_df.rename({pair[0]: 'Org Unit Code', pair[1]: 'Org Unit Name'}, axis=1, inplace=True)
            unit_df = unit_df[unit_df['Org Unit Code'].notnull()]
            org_name_lookup_df = org_name_lookup_df.append(unit_df)

        org_name_lookup_df.drop_duplicates(inplace=True)
        lookup_dict = pd.Series(org_name_lookup_df['Org Unit Name'].values,
                                index=org_name_lookup_df['Org Unit Code']).to_dict()
        return lookup_dict

    @staticmethod
    def map_org_names_to_flat_org_structure(flat_org_df: pd.DataFrame, lookup_dict: Mapping) -> pd.DataFrame:
        flat_org_name_df = flat_org_df.copy()
        flat_org_name_df['Org Line'] = flat_org_name_df['Org Line'].apply(lambda x: [lookup_dict[node] for node in x])

        return flat_org_name_df

    def create_org_id_df(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        organizational_dict = {}
        employee_list = list(self.orgs_for_ff[self.identifier])
        organizational_line = [self.get_parents(employee, organizational_dict) for employee in employee_list]

        org_dict = {'Org Line': organizational_line}
        flat_org_df = pd.DataFrame(org_dict)
        flat_org_df['Organizational Level'] = flat_org_df['Org Line'].apply(lambda x: len(x) - 3 if len(x) > 2 else 0)
        flat_org_id_df = flat_org_df['Org Line'].apply(pd.Series)
        flat_org_id_df = pd.concat([flat_org_id_df, flat_org_df[['Organizational Level']]], axis='columns')
        flat_org_id_df.columns = ["Level_" + str(column) if column != 'Organizational Level'
                                  else column for column in flat_org_id_df.columns]

        return flat_org_df, flat_org_id_df

    def create_org_name_df(self, flat_org_df: pd.DataFrame) -> pd.DataFrame:
        lookup_dict = self.create_org_unit_name_map_dictionary(self.ec_input_data)
        flat_org_name_df = self.map_org_names_to_flat_org_structure(flat_org_df, lookup_dict)
        flat_org_name_df = flat_org_name_df['Org Line'].apply(pd.Series)
        flat_org_name_df.columns = ["Level_" + str(column) + "_Name" for column in flat_org_name_df.columns]

        return flat_org_name_df

    def create_flat_file(self) -> pd.DataFrame:
        employee_df = self.create_employee_df()
        flat_org_df, flat_org_id_df = self.create_org_id_df()
        flat_org_name_df = self.create_org_name_df(flat_org_df)
        flat_file = pd.concat([employee_df, flat_org_id_df, flat_org_name_df], axis='columns')

        return flat_file


