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


class RD3rdDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_mxp_employees(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Manager User Sys ID', 'Direct Manager', 'Seniority in Years',
                      'Department Unit Department Unit Name', 'Legal Company Legal Company Code',
                      'Group Band Code', 'Job Title', 'IPE IPE Code']

        legal_company_criteria = ['MXP']

        mxp_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, legal_company_criteria, field_list)

        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

        field_list = ['User/Employee ID', 'Formal Name', 'Employment Details Termination Date',
                      'Department Unit Department Unit Name', 'Legal Company Legal Company Code', 'Reason for leaving']

        mxp_turnover_df = turnover_data[(turnover_data['Legal Company Legal Company Code']).isin(legal_company_criteria)
                                        & (turnover_data['Employment Details Termination Date'].dt.year == year) &
                                        (turnover_data['Employment Details Termination Date'].dt.month == month)][
            field_list]

        hc_sum = pd.crosstab(mxp_df['Legal Company Legal Company Code'], mxp_df['Legal Company Legal Company Code'])
        to_sum = pd.crosstab(mxp_turnover_df['Legal Company Legal Company Code'], mxp_turnover_df['Reason for leaving'])
        turover_rate_df = pd.concat([hc_sum, to_sum], axis='columns')
        if 'Voluntary' not in turover_rate_df.columns:
            turover_rate_df['Voluntary'] = 0
        if 'Involuntary' not in turover_rate_df.columns:
            turover_rate_df['Involuntary'] = 0

        turover_rate_df['Involuntary %'] = turover_rate_df['Involuntary'] / turover_rate_df['MXP']
        turover_rate_df['Voluntary %'] = turover_rate_df['Voluntary'] / turover_rate_df['MXP']
        turover_rate_df['Total %'] = turover_rate_df['Involuntary %'] + turover_rate_df['Voluntary %']

        path = '03_TEMPLATE MXP Employees.xlsx'
        o_name = 'MXP Employees + Turnover'
        df_dict = {'Headcount': mxp_df, 'Turnover': mxp_turnover_df, 'Turnover rate': turover_rate_df}
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_ytd_newhire_pdd(self):
        headcount_data = self.dataprovider.get_headcount_data()
        year = date.today().year
        field_list = ['User/Employee ID', 'Formal Name', 'Employment Details Anniversary Date',
                      'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                      'Legal Company Legal Company Code', 'Job Title', 'Job Role Job Role Name', 'Manager User Sys ID',
                      'Direct Manager']
        legal_company_criteria = ['GNO', 'GSF', 'GSV', 'GCI', 'GMU']

        pdd_df = headcount_data[((headcount_data['Legal Company Legal Company Code']).isin(legal_company_criteria)) &
                                (headcount_data['Employment Details Anniversary Date'].dt.year == year)][field_list]

        path = '03_TEMPLATE YTD New hires.xlsx'
        o_name = 'YTD New Hires - PDD'
        df_dict = {'Report': pdd_df}
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_apac_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        legal_company_criteria = ['GCH', 'GCQ', 'GHK', 'GPC', 'GPP', 'GPV', 'GSH', 'GTH', 'GWC', 'GAS', 'GPM', 'GSI',
                                  'GTI', 'GJK', 'GPK', 'GNZ', 'GPA', 'GTS', 'GTW', 'SSG', 'GIS', 'GAH', 'BKB', 'GPQ']
        field_list = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                      'Department Unit Department Unit Name', 'Job Role Job Role Name', 'Manager User Sys ID',
                      'Direct Manager', 'Group Band Code', 'Country/Region']

        apac_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, legal_company_criteria, field_list)

        df_dict = {'Report': apac_df}
        path = '03_TEMPLATE APAC Headcount.xlsx'
        o_name = 'APAC Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_hrbp_summary_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['HR Business Partner Job Relationships Name', 'Level_0_Name', 'Level_1_Name', 'Level_2_Name',
                      'Level_3_Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name', 'Level_7_Name', 'Level_8_Name',
                      'Level_9_Name', 'Level_10_Name', 'Level_11_Name']

        hrbp_complete = headcount_data[field_list]

        df_dict = {'Data': hrbp_complete}
        path = '03_TEMPLATE HRBP Summary Report.xlsx'
        o_name = 'HRBP Summary Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gsse_seniority_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        fields = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code',
                  'Employment Details Anniversary Date']
        company = ['GSSE']

        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company, fields)

        df_dict = {'Report': df}
        path = '03_TEMPLATE GSSE Seniority Report.xlsx'
        o_name = 'GSSE Seniority Report'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_gpc_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        fields = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Job Title',
                  'Department Unit Department Unit Name', 'Direct Manager',
                  'Group Band Code', 'Cost Centre Cost Center Code']
        company = ['GPC']

        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company, fields)

        df_dict = {'Report': df}
        path = '03_TEMPLATE GPC Headcount.xlsx'
        o_name = 'GPC Headcount'
        output_name = export_df(df_dict, path, o_name)
        return output_name

    @measure_running_time
    def rec_turnover_monitoring_by_cxo(self):
        turnover_data = self.dataprovider.get_turnover_data()
        fields = ['Employee Group', 'User/Employee ID', 'Formal Name', 'Global ID',
                  'Management Unit Management Unit Name',
                  'Function Unit Function Unit Name', 'Group Band Code', 'Age in Years', 'Subarea',
                  'Is People Manager Y/N',
                  'Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'Gender',
                  'Job Role Job Role Name',
                  'Employment Details Termination Date', 'Manager User Sys ID', 'Direct Manager',
                  'Employment Details Legal Date',
                  'Event Reason', 'Reason for leaving']
        df = turnover_data[fields].copy()

        df['Age Range'] = pd.cut(df['Age in Years'], bins=[0, 20, 30, 40, 50, 60, 70, 200], include_lowest=True,
                                 labels=['< 20', '20 - 29', '30 - 39', '40 - 49', '50 - 59', '60 - 69', '>= 70'])

        df_2021 = df[
            (df['Employment Details Termination Date'].dt.year == 2021) & (df['Employee Group'] != '9-External')].copy()

        df_2021['Month/Year'] = df_2021['Employment Details Termination Date'].apply(
            lambda x: str(x.month) + '-' + str(x.year))

        df_2021.drop(['Employee Group', 'Event Reason', 'Age in Years', 'Employment Details Termination Date'], axis=1,
                     inplace=True)

        df_2021 = df_2021[['User/Employee ID', 'Formal Name', 'Global ID', 'Management Unit Management Unit Name',
                           'Function Unit Function Unit Name', 'Group Band Code', 'Age Range', 'Subarea',
                           'Is People Manager Y/N',
                           'Legal Company Legal Company Code', 'Legal Company Legal Company Name', 'Gender',
                           'Job Role Job Role Name',
                           'Month/Year', 'Manager User Sys ID', 'Direct Manager', 'Employment Details Legal Date',
                           'Reason for leaving']]

        cxo_list = ['Operations (COO)', 'Finance (CFO)', 'Commercial (CCO)', 'Global Technology & Development (CTO)',
                    'HR (CHRO)']

        for cxo in cxo_list:
            print(f"Saving {cxo} version")
            output = df_2021[df_2021['Management Unit Management Unit Name'] == cxo].copy()
            df_dict = {'Data': output}
            path = '03_TEMPLATE Turnover Monitoring by CxO.xlsx'
            o_name = f'Turnover Monitoring {cxo}'
            output_name = export_df(df_dict, path, o_name)
            return output_name






