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
from ReportingSourceCode.SupportFunctions.extract_employee_data_changes import extract_employee_data_changes


class RD6thDayMonthly(IReportDefinition):
    def __init__(self, dataprovider: IDataProvider):
        super().__init__(dataprovider)

    @measure_running_time
    def rec_group_report(self):
        exp_data = self.dataprovider.get_expanded_ec_data()
        fields = ['Employment Details Is Contingent Worker', 'User/Employee ID', 'Global ID', 'First Name', 'Last Name',
                  'Job Title',
                  'Employee Status', 'Legal Company Legal Company Code', 'Organisational Company Code',
                  'Location Location Name',
                  'Direct Manager', 'IPE IPE Code', 'Employee Group', 'Employment Type', 'Cost Centre Cost Center Code',
                  'Department Unit Department Unit Code', 'Department Unit Department Unit Name',
                  'Function Unit Function Unit Code',
                  'Function Unit Function Unit Name', 'Management Unit Management Unit Code',
                  'Management Unit Management Unit Name',
                  'Manager User Sys ID', 'Job Role Job Role Code', 'Job Role Job Role Name',
                  'Job Cluster Job Cluster Code',
                  'Job Cluster Job Cluster Name', 'Job Family Code', 'Job Family Name', 'Subarea',
                  'Employee Sub Group Code',
                  'Group Band Code', 'Country/Region', 'HR Business Partner Job Relationships User ID', 'FTE',
                  'Direct Subordinates']

        df = exp_data[exp_data['Employee Status'] != 'Terminated'][fields].copy()

        template_path = '03_TEMPLATE Group Report.xlsx'
        o_name = 'Group Report'
        df_dict = {'Report': df}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_cto_hc(self):
        headcount_data = self.dataprovider.get_headcount_data()
        field_list = ['User/Employee ID', 'Formal Name', 'Gender', 'Age in Years', 'Date of Birth',
                      'Seniority in Years', 'Group Band Code', 'Employment Details Anniversary Date',
                      'Location Location Name', 'Employee Group', 'Department Unit Department Unit Code',
                      'Department Unit Department Unit Name', 'Subarea', 'Job Cluster Job Cluster Name',
                      'Job Family Name', 'Job Role Job Role Name', 'Manager User Sys ID', 'Direct Manager',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Department Unit Department Unit Short Name', 'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name', 'Employment Type', 'Job Title', 'Employee Sub Group Name', 'FTE']

        lvl_3_filter = 'Global Technology & Development (CTO)'
        emp_group_filter = ['1-Internal']

        # 1st CTO output
        cto_df = headcount_data[(headcount_data['Level_3_Name'] == lvl_3_filter) &
                                (headcount_data['Employee Group'].isin(emp_group_filter))][field_list]

        # 2nd CTO output
        field_list_2 = ['User/Employee ID', 'Formal Name', 'Gender', 'Level_3_Name', 'Level_4_Name', 'Level_5_Name',
                        'Level_6_Name', 'Level_7_Name', 'Level_8_Name', 'Level_9_Name', 'FTE']

        test = headcount_data[(headcount_data['Level_3_Name'] == lvl_3_filter) &
                              (headcount_data['Employee Group'].isin(emp_group_filter))][field_list_2].copy()

        cto_org_hier = test.melt(id_vars=['User/Employee ID', 'Formal Name', 'Gender', 'FTE'],
                                 value_vars=['Level_3_Name', 'Level_4_Name', 'Level_5_Name', 'Level_6_Name',
                                             'Level_7_Name', 'Level_8_Name', 'Level_9_Name']).sort_values(
            ['Formal Name', 'variable'])
        cto_org_hier = cto_org_hier[(cto_org_hier['Formal Name'].notnull()) & (cto_org_hier['value'].notnull())][
            ['User/Employee ID', 'Formal Name', 'Gender', 'value', 'FTE']]
        cto_org_hier.loc[cto_org_hier['value'] != lvl_3_filter, 'User/Employee ID'] = np.nan
        cto_org_hier.loc[cto_org_hier['value'] != lvl_3_filter, 'Formal Name'] = np.nan
        cto_org_hier.loc[cto_org_hier['value'] != lvl_3_filter, 'Gender'] = np.nan

        template_path = '03_TEMPLATE CTO Org Unit Report.xlsx'
        o_name = 'CTO Org unit report'
        df_dict = {'Page 1 - Employee list': cto_df, 'Page 3 - Org assignment': cto_org_hier}
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_service_hc_and_turnover(self):
        headcount_data = self.dataprovider.get_headcount_data()
        turnover_data = self.dataprovider.get_turnover_data()
        org_criteria = [50171652]
        field_list = ['Global ID', 'User/Employee ID', 'Displayed First Name', 'Last Name', 'Formal Name',
                      'Business Email Information Email Address', 'Job Role Job Role Name',
                      'Department Unit Department Unit Name', 'Initials', 'Job Title',
                      'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                      'Country/Region', 'Cell phone Formatted Phone Number', 'Manager User Sys ID', 'Direct Manager',
                      'Cost Centre Cost Center Code', 'Cost Centre Cost Center Name']

        ss_final_df = headcount_data[((headcount_data['Level_4']).isin(org_criteria))][field_list].copy()

        # Turnover
        year = date.today().year
        month = date.today().month
        if month == 1:
            year = year - 1
            month = 12
        else:
            month = month - 1

        turnover_field_list = ['User/Employee ID', 'Formal Name', 'Job Role Job Role Name',
                               'Department Unit Department Unit Code', 'Initials',
                               'Legal Company Legal Company Code', 'Legal Company Legal Company Name',
                               'Manager User Sys ID', 'Direct Manager', 'Employment Details Termination Date']

        ss_turnover_df = turnover_data[((turnover_data['Level_4']).isin(org_criteria)) &
                                       (turnover_data['Employment Details Termination Date'].dt.year == year) &
                                       (turnover_data['Employment Details Termination Date'].dt.month == month)][
            turnover_field_list]

        df_dict = {'Headcount': ss_final_df, 'Leavers': ss_turnover_df}
        template_path = '03_TEMPLATE Service EEWAA WEREG DACH.xlsx'
        o_name = 'Service Headcount and Turnover'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name

    @measure_running_time
    def rec_wu_headcount(self):
        headcount_data = self.dataprovider.get_headcount_data()
        org_filter = ['WU Solutions & Marketing', 'WU Sales']
        field_list = ['User/Employee ID', 'Formal Name', 'Job Role Job Role Name', 'Job Title', 'Group Band Code',
                      'Level_4_Name', 'HR Business Partner Job Relationships Name', 'Cost Centre Cost Center Code',
                      'Cost Centre Cost Center Name']

        wu_hc_df = headcount_data[headcount_data['Level_4_Name'].isin(org_filter)][field_list]

        df_dict = {'Report': wu_hc_df}
        template_path = '03_TEMPLATE WU Headcount Report.xlsx'
        o_name = 'WU Headcount Report'
        output_name = export_df(df_dict, template_path, o_name)
        return output_name








