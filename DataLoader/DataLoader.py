import pandas as pd
import numpy as np
from datetime import date
from ReportingSourceCode.DataLoader.data_file_paths import region_path, ec_hc_path, previous_month_ec_hc_path
from ReportingSourceCode.DataTransformation.FlatFileGenerator import FlatFileGenerator
from ReportingSourceCode.DataTransformation.CreateManagerLineDataFrame import CreateManagerLineDataFrame


class DataLoader:

    def __init__(self):
        self.region_path = region_path

        self.raw_ec_df = self.load_ec_data(ec_hc_path)
        self.recurring_region_df = self.load_region_data()
        self.ec_flat_file = self.create_flat_file_input(self.raw_ec_df)
        self.manager_line_df = self.create_manager_line_data()

        self.expanded_ec_df = self.expand_ec_raw_data(self.raw_ec_df, self.ec_flat_file)
        self.ec_headcount_df = self.create_ec_headcount_data(self.expanded_ec_df)
        self.ec_salary_df = self.create_ec_salary_data()
        self.ec_turnover_df = self.create_ec_turnover_data()
        self.ec_rehire_df = self.create_ec_rehire_data()
        self.previous_month_ec_headcount_df = self.create_previous_month_ec_headcount_data()

    def load_ec_data(self, data_path: str) -> pd.DataFrame:
        raw_ec_df = pd.read_excel(data_path)

        ids = ['User/Employee ID', 'Manager User Sys ID', 'Global ID']
        for col in ids:
            raw_ec_df[col] = pd.to_numeric(raw_ec_df[col], errors='coerce')

        dates = ['Employment Details Termination Date', 'Contract End Date', 'Date of Birth']
        for col in dates:
            raw_ec_df[col] = pd.to_datetime(raw_ec_df[col], errors='coerce')

        return raw_ec_df

    def load_region_data(self) -> pd.DataFrame:
        region_df = pd.read_excel(self.region_path)

        return region_df

    def expand_ec_raw_data(self, raw_ec_df: pd.DataFrame, ec_flat_file: pd.DataFrame) -> pd.DataFrame:
        """ This method is to add the extra fields to the base EC hc dataset"""
        ec_exp = raw_ec_df.merge(self.recurring_region_df[['Legal Company code', 'Region 2']], how='left',
                                  left_on='Legal Company Legal Company Code', right_on='Legal Company code')
        ec_exp.drop(['Legal Company code'], axis=1, inplace=True)

        ec_exp.columns = [col.replace("  ", " ") for col in ec_exp.columns]

        # Adding extra columns
        pd_today = pd.to_datetime(date.today())
        ec_exp['Contract Type'] = ec_exp['Contract End Date'].apply(
            lambda x: 'Fixed term' if pd.notna(x) else 'Regular')
        ec_exp['Age in Years'] = (pd_today - ec_exp['Date of Birth']) / np.timedelta64(1, 'Y')
        ec_exp['Country Code'] = ec_exp['Location Location Name'].apply(lambda x: str(x)[0:str(x).find("-")])
        ec_exp['Days in Contract'] = (pd_today - ec_exp['Employment Details Legal Date']) / np.timedelta64(1, 'D')
        ec_exp['Is People Manager Y/N'] = ec_exp['Direct Subordinates'].apply(lambda x: 'Yes' if x > 0 else 'No')
        ec_exp['Seniority in Years'] = (pd_today - ec_exp['Employment Details Legal Date']) / np.timedelta64(1, 'Y')

        ec_exp = ec_exp.merge(ec_flat_file, how='left', on=FlatFileGenerator.identifier)

        return ec_exp

    @staticmethod
    def create_ec_headcount_data(expanded_ec_df) -> pd.DataFrame:
        fields_to_exclude = ['Pay Component', 'Frequency', 'Currency', 'Amount', 'Event', 'Event Date']
        fields_for_headcount = [i for i in list(expanded_ec_df.columns) if i not in fields_to_exclude]

        ec_headcount_df = expanded_ec_df[expanded_ec_df['Employee Status'] == 'Active'][fields_for_headcount].copy()
        ec_headcount_df.drop_duplicates(inplace=True)

        return ec_headcount_df

    def create_ec_salary_data(self) -> pd.DataFrame:
        salary_df_fields = ['User/Employee ID', 'Global ID', 'Pay Component', 'Frequency', 'Currency', 'Amount',
                            'Event', 'Event Date']
        ec_salary_df = self.raw_ec_df[salary_df_fields].copy()

        return ec_salary_df

    def create_ec_turnover_data(self) -> pd.DataFrame:
        turnover_df = self.expanded_ec_df[self.expanded_ec_df['Employee Status'] == 'Terminated'].copy()
        turnover_df['Reason for leaving'] = turnover_df['Event Reason'].apply(
            lambda x: "Voluntary" if "vol/" in x else "Involuntary")
        turnover_df.drop_duplicates(subset='User/Employee ID', inplace=True)

        return turnover_df

    def create_ec_rehire_data(self) -> pd.DataFrame:
        rehire_df = self.expanded_ec_df[self.expanded_ec_df['Event'] == 'Rehire'].copy()
        rehire_df.drop_duplicates(subset='User/Employee ID', inplace=True)

        return rehire_df

    def create_org_hierarchy_data(self) -> pd.DataFrame:
        """ TODO: This method is to generate the full org hierarchy with summed headcount """
        pass

    @staticmethod
    def create_flat_file_input(raw_ec_df: pd.DataFrame) -> pd.DataFrame:
        flat_file_generator = FlatFileGenerator(raw_ec_df)
        ec_flat_file = flat_file_generator.create_flat_file()

        return ec_flat_file

    def create_manager_line_data(self) -> pd.DataFrame:
        man_line_generator = CreateManagerLineDataFrame(self.raw_ec_df)
        manager_line_df = man_line_generator.create_manager_line_to_employee_dataframe()

        return manager_line_df

    def create_previous_month_ec_headcount_data(self) -> pd.DataFrame:
        pm_raw_ec_df = self.load_ec_data(previous_month_ec_hc_path)
        pm_ec_flat_file = self.create_flat_file_input(pm_raw_ec_df)
        pm_expanded_ec_df = self.expand_ec_raw_data(pm_raw_ec_df, pm_ec_flat_file)
        pm_ec_headcount_df = self.create_ec_headcount_data(pm_expanded_ec_df)

        return pm_ec_headcount_df


