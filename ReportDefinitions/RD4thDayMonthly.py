import pandas as pd
from ReportingSourceCode.ReportDefinitions.IReportDefinition import IReportDefinition
from ReportingSourceCode.DataProvider.IDataProvider import IDataProvider
from ReportingSourceCode.SupportFunctions.export_dataframes_to_excel import (export_df,
                                                                             export_df_with_header,
                                                                             export_df_with_header_and_index)
from ReportingSourceCode.SupportFunctions.filter_dataframes import filter_dataframe_on_legal_company_and_field_name
from ReportingSourceCode.SupportFunctions.decorators import measure_running_time


class RD4thDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_tb_comp_tb_cbs_szf_ind_szf_wu_turnover(self):
        turnover_data = self.dataprovider.get_turnover_data()
        org_criteria = [50173013, 50173077, 50172989, 50172956]

        fields = ['User/Employee ID', 'Formal Name', 'Reason for leaving', 'Event Reason',
                  'Employment Details Termination Date', 'Department Unit Department Unit Name']

        tb_szf_turnover = turnover_data[turnover_data['Level_6'].isin(org_criteria)][fields].copy()

        df_dict = {'Report': tb_szf_turnover}
        path = '03_TEMPLATE TB Comp TB CBS SZF IND SZF WU Turnover.xlsx'
        o_name = 'TB Comp TB CBS SZF IND SZF WU Turnover'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gms_monthly_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Employee Status', 'Legal Company Legal Company Code']
        lc_criteria = ['GMS']
        gms_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, lc_criteria, field_list)

        df_dict = {'Report': gms_df}
        path = '03_TEMPLATE GMS monthly headcount.xlsx'
        o_name = 'GMS Monthly Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gsse_position_development(self):
        expanded_data = self.dataprovider.get_expanded_ec_data()
        df = expanded_data[expanded_data['Legal Company Legal Company Code'] == 'GSSE'].copy()
        output = pd.crosstab(df['Department Unit Department Unit Name'], df['Employee Status'], margins=True).reset_index()

        df_dict = {'Report': output}
        path = '03_TEMPLATE GSSE Position Development.xlsx'
        o_name = 'GSSE Position Development'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_dk_long_term_sickness_tool_data(self):
        headcount_data = self.dataprovider.get_headcount_data()
        fields = ['User/Employee ID', 'Formal Name', 'Employee Group', 'Job Role Job Role Name',
                  'Employee Sub Group Code',
                  'Employee Sub Group Name', 'Gender', 'Date of Birth', 'Employment Details Legal Date',
                  'Employment Details Anniversary Date',
                  'Legal Company Legal Company Code', 'Management Unit Management Unit Name',
                  'Department Unit Department Unit Short Name', 'Department Unit Department Unit Code',
                  'Department Unit Department Unit Name', 'Manager User Sys ID', 'Direct Manager',
                  'Business Email Information Email Address', 'Home Postal Code', 'Home City']

        df = headcount_data[headcount_data['Country/Region'] == 'Denmark'][fields]

        df_dict = {'Report': df}
        path = '03_TEMPLATE DK Long Term Sickness Tool Data.xlsx'
        o_name = 'DK Long Term Sickness Tool Data'
        output_name = export_df(df_dict, path, o_name)
        return output_name








