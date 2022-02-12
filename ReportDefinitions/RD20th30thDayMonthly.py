from datetime import date
import pandas as pd
import numpy as np
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
import datetime
from ReportingSourceCode.SupportFunctions.extract_employee_data_changes import extract_employee_data_changes


class RD20th30thDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_dk_monthly_turnover(self):
        turnover_data = self.dataprovider.get_turnover_data()
        company_filter = ['GBB', 'GBJ', 'GDK', 'GFI', 'GLL', 'GMA', 'GOE', 'PDJ', 'SEN', 'STX']
        field_list = ['User/Employee ID', 'Formal Name', 'Reason for leaving', 'Event Reason',
                      'Employment Details Termination Date']
        year = date.today().year
        month = date.today().month

        turnover = filter_turnover_dataframe_on_year_and_month(turnover_data, company_filter, field_list, year, month)

        df_dict = {'Report': turnover}
        template_path = '03_TEMPLATE DK Monthly Turnover.xlsx'
        o_name = 'DK Monthly Turnover'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gms_fte_summary(self):
        headcount_data = self.dataprovider.get_headcount_data()
        fields = ['User/Employee ID', 'Formal Name', 'Subarea', 'Organisational Company Code',
                  'Legal Company Legal Company Code', 'FTE', 'Level_3_Name', 'Level_4_Name', 'Level_5_Name',
                  'Level_6_Name', 'Level_7_Name', 'Level_8_Name', 'Level_9_Name', 'Level_10_Name', 'Level_11_Name']

        gms_df = headcount_data[(headcount_data['Legal Company Legal Company Code'] == 'GMS') &
                                (headcount_data['Organisational Company Code'].isna())][fields]

        gms_org_sum = gms_df[['Level_3_Name', 'Level_4_Name', 'Subarea', 'FTE']].groupby(
            ['Level_3_Name', 'Level_4_Name', 'Subarea']).sum().reset_index()

        df_dict = {'Headcount': gms_df, 'Organization summary': gms_org_sum}
        template_path = '03_TEMPLATE GMS FTE Summary.xlsx'
        o_name = 'GMS FTE Summary'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gbj_gma_gdk_gfi_sen_gbb_monthy_turnover(self):
        turnover_data = self.dataprovider.get_turnover_data()
        company_filter = ['GBJ', 'GMA', 'GDK', 'GFI', 'SEN', 'GBB']
        field_list = ['User/Employee ID', 'Formal Name', 'Reason for leaving', 'Event Reason',
                      'Employment Details Termination Date', 'Legal Company Legal Company Code']
        year = date.today().year

        turnover_df = turnover_data[(turnover_data['Employment Details Termination Date'].dt.year == year) &
                                    (turnover_data['Legal Company Legal Company Code'].isin(company_filter))][
            field_list].copy()

        df_dict = {'Report': turnover_df}
        template_path = '03_TEMPLATE Global Monthly Turnover.xlsx'
        o_name = 'GBJ GMA GDK GFI SEN GBB Monthly Turnover'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gfd_cost_center_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        fields = ['User/Employee ID', 'Formal Name', 'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name',
                  'Legal Company Legal Company Code']
        company = ['GFD']
        df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company, fields)

        df_dict = {'Report': df}
        template_path = '03_TEMPLATE GFD Cost Center Report.xlsx'
        o_name = 'GFD Cost Center Report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_collar_position_check(self):
        headcount_data = self.dataprovider.get_headcount_data()
        prev_hc = self.dataprovider.get_previous_month_headcount_data()

        field_list = ['User/Employee ID', 'Formal Name', 'Global ID', 'Subarea', 'Employment Details Anniversary Date',
                      'Employment Details Legal Date', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Employee Group', 'Location Location Name',
                      'Employee Sub Group Name', 'Group Band Code', 'Job Family Name', 'Job Cluster Job Cluster Name',
                      'Job Role Job Role Name', 'Legal Company Legal Company Code', 'Organisational Company Code',
                      'Legal Company Legal Company Name', 'Manager User Sys ID', 'Direct Manager',
                      'Job Title', 'Is People Manager Y/N', 'Level_3_Name', 'Level_4_Name', 'Level_5_Name',
                      'Level_6_Name', 'STI Target %', 'STI Max %']

        curr_data = headcount_data[field_list].copy()
        prev_data = prev_hc[field_list].copy()
        df = extract_employee_data_changes(curr_data, prev_data, field_list)
        collar_compare_output_df = df[df['Subarea Change']]
        position_compare_df_output = df[df['Job Title Change']]

        df_dict = {'Blue to White Collar': collar_compare_output_df, 'Position Changes': position_compare_df_output}
        template_path = '03_TEMPLATE HC Collar and Position Change.xlsx'
        o_name = 'Blue to White collar and Position Changes'
        output_name = export_df_with_header(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_employees_with_2_ids(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['Global ID', 'User/Employee ID', 'Formal Name', 'Subarea', 'Employment Details Anniversary Date',
                      'Employment Details Legal Date', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Employee Group', 'Location Location Name',
                      'Employee Sub Group Name', 'Group Band Code', 'Job Family Name', 'Job Cluster Job Cluster Name',
                      'Job Role Job Role Name', 'Legal Company Legal Company Code', 'Organisational Company Code',
                      'Legal Company Legal Company Name', 'Manager User Sys ID', 'Direct Manager',
                      'Job Title', 'Is People Manager Y/N', 'Level_3_Name', 'Level_4_Name', 'Level_5_Name',
                      'Level_6_Name', 'STI Target %', 'STI Max %']

        g_a_id_check_df = headcount_data[headcount_data['User/Employee ID'] != headcount_data['Global ID']][field_list]

        df_dict = {'Report': g_a_id_check_df}
        template_path = '03_TEMPLATE Employees with Two IDs.xlsx'
        o_name = 'Employees with Two IDs'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_china_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['Global ID', 'Formal Name', 'Initials', 'Location Location Name',
                      'Legal Company Legal Company Code', 'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name', 'Group Band Code',
                      'Department Unit Department Unit Code', 'Department Unit Department Unit Name', 'Level_4_Name',
                      'Level_5_Name', 'Level_6_Name', 'Level_7_Name', 'Level_8_Name', 'Level_9_Name', 'Level_10_Name',
                      'Level_11_Name', 'Employment Details Anniversary Date', 'Manager User Sys ID',
                      'Direct Manager', 'Job Title', 'Job Role Job Role Name',
                      'HR Business Partner Job Relationships Name']
        field_list_2 = ['Global ID', 'Formal Name', 'Initials', 'Location Location Name',
                        'Legal Company Legal Company Code', 'Cost Centre Cost Center Code', 'Group Band Code',
                        'Job Title', 'Job Role Job Role Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name',
                        'Level_7_Name', 'Level_8_Name', 'Level_9_Name', 'Level_10_Name', 'Level_11_Name',
                        'Manager User Sys ID', 'Direct Manager', 'Contract End Date',
                        'HR Business Partner Job Relationships User ID', 'HR Business Partner Job Relationships Name']
        company_criteria = ['GCH', 'GSH', 'GCQ', 'GHK', 'GPC', 'GWC']

        gch_flat_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_criteria, field_list)
        ch2_df = filter_dataframe_on_legal_company_and_field_name(headcount_data, company_criteria, field_list_2)

        df_dict = {'Report': gch_flat_df, 'Report version 2': ch2_df}
        template_path = '03_TEMPLATE China Headcount.xlsx'
        o_name = 'China Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_gsse_external_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        legal_company_filter = 'GSSE'
        field_list = ['User/Employee ID', 'First Name', 'Last Name', 'Legal Company Legal Company Code',
                      'Employment Details Anniversary Date', 'Manager User Sys ID', 'Direct Manager']
        employee_group_filter = '9-External'

        gsse_external_df = headcount_data[(headcount_data['Legal Company Legal Company Code'] == legal_company_filter) &
                                          (headcount_data['Employee Group'] == employee_group_filter)][field_list]
        manager_df = get_manager_data(headcount_data, ['Business Email Information Email Address'])
        gsse_external_df = gsse_external_df.merge(manager_df, how='left', on='Manager User Sys ID')

        df_dict = {'Report': gsse_external_df}
        template_path = '03_TEMPLATE GSSE External Headcount.xlsx'
        o_name = 'GSSE External Headcount'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_group_sti_report(self):
        headcount_data = self.dataprovider.get_headcount_data()
        sti_fields = ['User/Employee ID', 'First Name', 'Last Name', 'Legal Company Legal Company Code', 'Subarea',
                      'Group Band Code',
                      'Cost Centre Cost Center Code', 'STI Target %', 'PG110-Amount', 'PG110-Currency']

        sti_df = headcount_data[sti_fields]

        df_dict = {'Report': sti_df}
        template_path = '03_TEMPLATE Group STI Report.xlsx'
        o_name = 'Group STI Report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name
