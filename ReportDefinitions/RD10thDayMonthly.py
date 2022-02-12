import datetime
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


class RD10thDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_hun_fixed_leavers(self):
        headcount_data = self.dataprovider.get_headcount_data()
        year = date.today().year
        month = date.today().month

        company_filter = ['GMH', 'GHU', 'GSSE']
        field_list = ['User/Employee ID', 'Formal Name', 'Employee Status', 'Legal Company Legal Company Code',
                      'Manager User Sys ID', 'Direct Manager', 'Contract Type', 'Employment Details Legal Date',
                      'Contract End Date']

        gmh_contract_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_filter, field_list)
        gmh_contract_df = gmh_contract_df[(gmh_contract_df['Contract End Date'].dt.year == year) &
                                          ((gmh_contract_df['Contract End Date'].dt.month == month) |
                                           (gmh_contract_df['Contract End Date'].dt.month == month + 1))]

        df_dict = {'Report': gmh_contract_df}
        template_path = '03_TEMPLATE Hungary Fixed Term Leavers.xlsx'
        o_name = 'Hungary Fixed Term Leavers'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_ch_flat_combined(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Initials', 'Formal Name', 'Location Location Name',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name', 'Group Band Code',
                      'Business Email Information Email Address', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name',
                      'Level_7_Name', 'Level_8_Name', 'Level_9_Name', 'Level_10_Name', 'Level_11_Name',
                      'Employment Details Anniversary Date', 'Manager User Sys ID', 'Direct Manager', 'Job Title',
                      'Job Role Job Role Name']
        company_criteria = ['GSH', 'GCH', 'GCQ', 'GPC', 'GWC', 'GHK', 'GTW', 'GTS']

        ch_flat_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_criteria, field_list)
        manager_cc_df = get_manager_data(headcount_data, ['Legal Company Legal Company Code'])

        ch_flat_merged_df = ch_flat_df.merge(manager_cc_df, how='left', on='Manager User Sys ID')

        ch_flat_merged_df = ch_flat_merged_df[
            ['User/Employee ID', 'Initials', 'Formal Name', 'Location Location Name',
             'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
             'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name', 'Group Band Code',
             'Business Email Information Email Address', 'Department Unit Department Unit Code',
             'Department Unit Department Unit Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name',
             'Level_7_Name', 'Level_8_Name', 'Level_9_Name', 'Level_10_Name', 'Level_11_Name',
             'Employment Details Anniversary Date', 'Manager User Sys ID', 'Direct Manager',
             'Manager Legal Company Legal Company Code', 'Job Title', 'Job Role Job Role Name']]

        df_dict = {'Report': ch_flat_merged_df}
        template_path = '03_TEMPLATE GSH GCH GCQ GPC GWC HC + Flat file combined.xlsx'
        o_name = 'GSH GCH GCQ GPC GWC HC + Flat file combined'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gth_gpv_gis_gpp_gsh_gch_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Initials', 'Formal Name', 'Location Location Name',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name', 'Group Band Code',
                      'Business Email Information Email Address', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name',
                      'Level_7_Name', 'Level_8_Name', 'Level_9_Name', 'Level_10_Name', 'Level_11_Name',
                      'Employment Details Anniversary Date', 'Manager User Sys ID', 'Direct Manager', 'Job Title',
                      'Job Role Job Role Name']
        company_criteria = ['GTH', 'GPV', 'GIS', 'GPP', 'GSH', 'GCH']

        apac_flat_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_criteria, field_list)
        manager_cc_df = get_manager_data(headcount_data, ['Legal Company Legal Company Code'])

        apac_flat_merged_df = apac_flat_df.merge(manager_cc_df, how='left', on='Manager User Sys ID')

        apac_flat_merged_df = apac_flat_merged_df[
            ['User/Employee ID', 'Initials', 'Formal Name', 'Location Location Name',
             'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
             'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name', 'Group Band Code',
             'Business Email Information Email Address', 'Department Unit Department Unit Code',
             'Department Unit Department Unit Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name',
             'Level_7_Name', 'Level_8_Name', 'Level_9_Name', 'Level_10_Name', 'Level_11_Name',
             'Employment Details Anniversary Date', 'Manager User Sys ID', 'Direct Manager',
             'Manager Legal Company Legal Company Code', 'Job Title', 'Job Role Job Role Name']]

        df_dict = {'Report': apac_flat_merged_df}
        template_path = '03_TEMPLATE GTH GPV GIS GPP GSH GCH Headcount.xlsx'
        o_name = 'GTH GPV GIS GPP GSH GCH Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gpa_cost_center_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        company = ['GPA']
        fields = ['User/Employee ID', 'Formal Name', 'Legal Company Legal Company Code', 'Cost Centre Cost Center Code',
                  'Cost Centre Cost Center Name']

        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company, fields)

        df_dict = {'Report': df}
        template_path = '03_TEMPLATE GPA Cost Center Report.xlsx'
        o_name = 'GPA Cost Center Report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_cfo_newcomers(self):
        headcount_data = self.dataprovider.get_headcount_data()

        curr_year = date.today().year
        prev_year = curr_year
        curr_month = date.today().month
        prev_month = curr_month - 1
        if curr_month == 1:
            prev_year = curr_year - 1
            prev_month = 12

        start = datetime.datetime(prev_year, prev_month, 1)
        end = datetime.datetime(curr_year, curr_month, 10)

        lvl_4_criteria = ["Finance Business Partnering", "Finance Services", "Group Finance Planning",
                          "Group Finance Reporting",
                          "Internal Audit"]
        fields = ['User/Employee ID', 'Formal Name', 'Employment Details Original Start Date',
                  'Level_4_Name']

        df = headcount_data[(headcount_data['Level_4_Name'].isin(lvl_4_criteria)) &
                            (headcount_data['Employment Details Anniversary Date'].between(start, end))][fields]

        df_dict = {'Report': df}
        template_path = '03_TEMPLATE CFO Newcomers.xlsx'
        o_name = 'CFO Newcomers'
        output_name = export_df_with_header(df_dict, template_path, o_name)
        return output_name
























