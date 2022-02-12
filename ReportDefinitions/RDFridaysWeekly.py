from datetime import date
import pandas as pd
from ReportingSourceCode.ReportDefinitions.IReportDefinition import IReportDefinition
from ReportingSourceCode.DataProvider.IDataProvider import IDataProvider
from ReportingSourceCode.SupportFunctions.export_dataframes_to_excel import (export_df,
                                                                             export_df_with_header,
                                                                             export_df_with_header_and_index)
from ReportingSourceCode.SupportFunctions.filter_dataframes import filter_dataframe_on_legal_company_and_field_name
from ReportingSourceCode.SupportFunctions.decorators import measure_running_time
from ReportingSourceCode.SupportFunctions.get_manager_data import get_manager_data
import os


class RDFridaysWeekly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_home_office_delivery_report(self):
        user_id_file = 'ReportingSourceCode/ReportDefinitions/temporary_files/Users for HRC0210641.xlsx'
        if "Tests" in os.getcwd():
            user_id_file = 'C:/Users/78595/PycharmProjects/recurring_reports/ReportingSourceCode/ReportDefinitions/temporary_files/Users for HRC0210641.xlsx'
        users_df = pd.read_excel(user_id_file)

        expanded_ec_data = self.dataprovider.get_expanded_ec_data()
        turnover_data = self.dataprovider.get_turnover_data()

        turnover = turnover_data[['User/Employee ID', 'Employment Details Termination Date']].copy()

        df = users_df.merge(turnover, how='left', on='User/Employee ID')

        status_df = expanded_ec_data[['User/Employee ID', 'Employee Status']].copy()
        status_df.drop_duplicates(inplace=True)

        df = df.merge(status_df, how='left', on='User/Employee ID')

        path = '03_TEMPLATE Home Office Delivery Report.xlsx'
        o_name = 'Home Office Delivery Report'
        df_dict = {'Report': df}
        output_name = export_df(df_dict, path, o_name)
        return output_name













