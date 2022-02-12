from datetime import date
import pandas as pd
from ReportingSourceCode.ReportDefinitions.IReportDefinition import IReportDefinition
from ReportingSourceCode.DataProvider.IDataProvider import IDataProvider
from ReportingSourceCode.SupportFunctions.export_dataframes_to_excel import (export_df,
                                                                             export_df_with_header,
                                                                             export_df_with_header_and_index)
from ReportingSourceCode.SupportFunctions.filter_dataframes import (filter_dataframe_on_legal_company_and_field_name,
                                                                    filter_turnover_dataframe_on_year_and_month)
from ReportingSourceCode.SupportFunctions.decorators import measure_running_time
from ReportingSourceCode.SupportFunctions.get_manager_data import get_manager_data
import os
from ReportingSourceCode.SupportFunctions.extract_employee_data_changes import extract_employee_data_changes


class RD7thDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_ytd_global_new_hires(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Global ID', 'Employment Details Anniversary Date',
                      'Employment Details Legal Date', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name',
                      'Employee Group', 'Location Location Name', 'Employee Sub Group Name', 'Group Band Code',
                      'Job Family Name', 'Job Cluster Job Cluster Name', 'Job Role Job Role Name',
                      'Legal Company Legal Company Code', 'Organisational Company Code',
                      'Legal Company Legal Company Name', 'Manager User Sys ID', 'Direct Manager', 'Job Title',
                      'Is People Manager Y/N', 'Level_3_Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name',
                      'Subarea', 'STI Target %', 'STI Max %', 'Employee Status']

        year = date.today().year
        pd_today = pd.Timestamp(date.today())

        base_newcomers_bw = headcount_data[(headcount_data['Employment Details Anniversary Date'].dt.year == year) &
                                           (headcount_data['Employment Details Anniversary Date'] <= pd_today) &
                                           (headcount_data['Employee Group'] != '9-External')][field_list]

        template_path = '03_TEMPLATE YTD Global New Hires.xlsx'
        o_name = 'YTD Global New Hires'
        df_dict = {'Report': base_newcomers_bw}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def suspended_gms_ops_hc(self):
        """ Not needed anymore"""
        headcount_data = self.dataprovider.get_headcount_data()
        lcc_filter = ['GMS']
        field_list = ['Global ID', 'User/Employee ID', 'Formal Name', 'Level_3_Name', 'Level_6_Name',
                      'Department Unit Department Unit Name',
                      'Manager User Sys ID', 'Direct Manager', 'Legal Company Legal Company Code',
                      'Contract Type', 'Employee Status', 'FTE']

        gms_hc_factory_df = headcount_data[(headcount_data['Legal Company Legal Company Code'].isin(lcc_filter)) &
                                           (headcount_data['Level_3_Name'] == 'Operations (COO)') &
                                           (headcount_data['Employee Status'] == 'Active')][field_list]

        gms_apu_df = gms_hc_factory_df[~gms_hc_factory_df['Level_6_Name'].isna()]
        gms_apu_df = gms_apu_df[gms_apu_df['Level_6_Name'].str.contains('APU')]

        df_dict = {'Ops HC': gms_hc_factory_df, 'Ops APU HC': gms_apu_df}
        template_path = '03_TEMPLATE GMS Ops Headcount.xlsx'
        o_name = 'GMS Ops Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name
